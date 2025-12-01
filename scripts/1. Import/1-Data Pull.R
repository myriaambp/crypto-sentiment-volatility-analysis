###############################
###  LIBRARIES
###############################
library(httr)
library(jsonlite)
library(dplyr)
library(purrr)
library(lubridate)
library(ggplot2)
library(gtrendsR)
library(scales)
library(readr)
library(zoo)



###############################
###  API KEYS FROM .Renviron
###############################
demo_key <- Sys.getenv("COINGECKO_KEY")
s_api_key <- Sys.getenv("SANTIMENT_KEY")

if (demo_key == "") stop("Missing COINGECKO_KEY in .Renviron")
if (s_api_key == "") stop("Missing SANTIMENT_KEY in .Renviron")



###############################
###  DIRECTORY SETUP
###############################
raw_dir   <- "data/raw"
clean_dir <- "data/clean"

dir.create(raw_dir,  showWarnings = FALSE, recursive = TRUE)
dir.create(clean_dir, showWarnings = FALSE, recursive = TRUE)



###############################
###  FETCH CRYPTO PRICES
###############################
fetch_crypto_data <- function(coin_id, vs_currency = "usd", days = "365", api_key = demo_key) {

  url <- paste0("https://api.coingecko.com/api/v3/coins/", coin_id, "/market_chart")

  res <- GET(
    url,
    query = list(
      vs_currency = vs_currency,
      days = days,
      x_cg_demo_api_key = api_key
    )
  )

  timestamp_str <- format(Sys.time(), "%Y-%m-%d_%H%M")
  raw_path <- file.path(raw_dir, paste0(toupper(coin_id), "_raw_", timestamp_str, ".csv"))

  payload <- tryCatch(fromJSON(rawToChar(res$content)), error = function(e) NULL)

  if (!is.null(payload)) {
    raw_combined <- bind_rows(
      data.frame(type = "prices",       payload$prices),
      data.frame(type = "market_caps",  payload$market_caps),
      data.frame(type = "total_volumes",payload$total_volumes)
    )
    names(raw_combined)[2:3] <- c("timestamp", "value")

    raw_combined <- raw_combined |>
      mutate(date = as.POSIXct(timestamp / 1000, origin = "1970-01-01", tz = "UTC"))

    write_csv(raw_combined, raw_path)
    message("Raw crypto file saved: ", raw_path)
  }

  # Clean
  if (status_code(res) == 200 && !is.null(payload)) {

    df_price <- as.data.frame(payload$prices)         |> rename(timestamp = 1, price      = 2)
    df_vol   <- as.data.frame(payload$total_volumes)  |> rename(timestamp = 1, volume     = 2)
    df_cap   <- as.data.frame(payload$market_caps)    |> rename(timestamp = 1, market_cap = 2)

    df <- reduce(list(df_price, df_vol, df_cap), merge, by = "timestamp") |>
      mutate(
        date = as.POSIXct(timestamp / 1000, origin = "1970-01-01", tz = "UTC"),
        coin = toupper(coin_id)
      ) |>
      select(date, coin, price, volume, market_cap)

    return(df)
  } else {
    message("Request failed for ", coin_id)
    return(NULL)
  }
}

# Fetch 1 year daily BTC + ETH
btc <- fetch_crypto_data("bitcoin")
eth <- fetch_crypto_data("ethereum")

crypto_all <- bind_rows(btc, eth)

write_csv(crypto_all, file.path(clean_dir, "crypto_prices_1year.csv"))



###############################
###  AUTO-DETECT START/END DATES
###############################
start_date <- as.Date(min(crypto_all$date, na.rm = TRUE))
end_date_raw <- as.Date(max(crypto_all$date, na.rm = TRUE))
end_date <- min(end_date_raw, Sys.Date())  # clamp to today

message("Auto-detected date range (clamped to today): ", start_date, " to ", end_date)



###############################
###  SENTIMENT DATA (Santiment API)
###############################
get_santiment_sentiment <- function(slug, from_date, to_date, api_key = s_api_key) {

  from_date <- as.Date(from_date)
  to_date   <- as.Date(to_date)

  if (to_date < from_date) {
    warning("For ", slug, ": to_date < from_date, skipping sentiment fetch.")
    return(NULL)
  }

  url <- "https://api.santiment.net/graphql"

  query <- paste0('{
    getMetric(metric: "sentiment_volume_consumed_total") {
      timeseriesData(
        slug: "', slug, '"
        from: "', format(from_date, "%Y-%m-%d"), 'T00:00:00Z"
        to: "',   format(to_date,   "%Y-%m-%d"), 'T00:00:00Z"
        interval: "1d"
      ) {
        datetime
        value
      }
    }
  }')

  res <- POST(
    url,
    add_headers("Authorization" = paste("Apikey", api_key)),
    body = list(query = query),
    encode = "json"
  )

  timestamp_str <- format(Sys.time(), "%Y-%m-%d_%H%M")
  raw_path <- file.path(raw_dir, paste0(toupper(slug), "_sentiment_raw_", timestamp_str, ".csv"))

  if (status_code(res) != 200) {
    warning("Sentiment request failed for ", slug, " (status ", status_code(res), ")")
    return(NULL)
  }

  raw_text <- content(res, "text", encoding = "UTF-8")
  raw_json <- fromJSON(raw_text, simplifyVector = TRUE)

  timeseries <- raw_json$data$getMetric$timeseriesData

  # HANDLE EMPTY OR WEIRD RESULTS
  if (is.null(timeseries) || length(timeseries) == 0) {
    warning("No sentiment data returned for ", toupper(slug))
    return(NULL)
  }

  raw_df <- as.data.frame(timeseries, stringsAsFactors = FALSE)

  if (!all(c("datetime", "value") %in% names(raw_df))) {
    warning(
      "Unexpected columns in sentiment response for ", toupper(slug),
      ": ", paste(names(raw_df), collapse = ", ")
    )
    return(NULL)
  }

  # save raw
  write_csv(raw_df, raw_path)
  message("Raw sentiment file saved: ", raw_path)

  # clean
  clean_df <- raw_df |>
    mutate(
      date = as.Date(datetime),
      coin = toupper(slug),
      sentiment_score = as.numeric(value)
    ) |>
    select(date, coin, sentiment_score)

  return(clean_df)
}

btc_sent <- get_santiment_sentiment("bitcoin",  start_date, end_date)
eth_sent <- get_santiment_sentiment("ethereum", start_date, end_date)

# bind only non-NULL sentiment data
sentiment_all <- bind_rows(Filter(Negate(is.null), list(btc_sent, eth_sent)))



###############################
###  GOOGLE TRENDS (AUTO DATE RANGE)
###############################
fetch_daily_trends <- function(keyword, start_date, end_date, geo = "US") {

  save_raw   <- raw_dir
  save_clean <- clean_dir

  start_date <- as.Date(start_date)
  end_date   <- as.Date(end_date)

  if (end_date < start_date) {
    warning("For ", keyword, ": end_date < start_date, skipping Google Trends.")
    return(NULL)
  }

  # Break into 90-day windows
  dates <- seq(start_date, end_date, by = "90 days")
  if (length(dates) < 2) dates <- c(start_date, end_date)

  ranges <- data.frame(
    start = head(dates, -1),
    end   = tail(dates, -1) - 1
  )

  trend_list <- map(1:nrow(ranges), function(i) {
    range_str <- paste(ranges$start[i], ranges$end[i])
    message("Fetching Google Trends for ", keyword, " : ", range_str)

    out <- tryCatch(
      gtrends(keyword = keyword, geo = geo, time = range_str)$interest_over_time,
      error = function(e) {
        warning("gtrends error for ", keyword, " in range ", range_str, ": ", e$message)
        NULL
      }
    )
    Sys.sleep(1) # be polite to the API
    out
  })

  all_data <- bind_rows(Filter(Negate(is.null), trend_list))

  if (nrow(all_data) == 0) {
    warning("No Google Trends data returned for ", keyword)
    return(NULL)
  }

  # standardize datetime column name
  if ("date" %in% names(all_data)) {
    all_data <- rename(all_data, datetime = date)
  } else if ("time" %in% names(all_data)) {
    all_data <- rename(all_data, datetime = time)
  } else {
    warning("No 'date' or 'time' column in Google Trends result for ", keyword)
    return(NULL)
  }

  final <- all_data |>
    mutate(
      date = as.Date(datetime),
      search_interest = as.numeric(replace(hits, hits == "<1", 0)),
      coin = toupper(keyword)
    ) |>
    select(date, coin, search_interest)

  write_csv(
    all_data,
    file.path(save_raw, paste0("google_trends_raw_", toupper(keyword), "_", Sys.Date(), ".csv"))
  )
  write_csv(
    final,
    file.path(save_clean, paste0("google_trends_clean_", toupper(keyword), "_", Sys.Date(), ".csv"))
  )

  message("Google Trends files saved for ", keyword)

  return(final)
}

btc_trends <- fetch_daily_trends("bitcoin",  start_date, end_date)
eth_trends <- fetch_daily_trends("ethereum", start_date, end_date)

trends_all <- bind_rows(Filter(Negate(is.null), list(btc_trends, eth_trends)))



###############################
###  MERGE ALL DATA SOURCES
###############################
merged <- crypto_all |>
  mutate(date = as.Date(date)) |>
  left_join(sentiment_all, by = c("date", "coin")) |>
  left_join(trends_all,    by = c("date", "coin")) |>
  group_by(coin) |>
  arrange(date, .by_group = TRUE) |>
  mutate(
    daily_return = price / lag(price) - 1,
    volatility_7d = rollapply(daily_return, 7, sd, fill = NA, align = "right")
  ) |>
  ungroup()

write_csv(merged, file.path(clean_dir, "merged_crypto_sentiment_trends.csv"))

message("Merged dataset saved to data/clean/merged_crypto_sentiment_trends.csv")
