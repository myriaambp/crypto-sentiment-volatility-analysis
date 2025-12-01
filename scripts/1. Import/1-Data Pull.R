
### Library

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

#### LOAD API KEYS FROM .Renviron ####
demo_key <- Sys.getenv("COINGECKO_KEY")
s_api_key <- Sys.getenv("SANTIMENT_KEY")

if (demo_key == "") stop("Missing COINGECKO_KEY in .Renviron")
if (s_api_key == "") stop("Missing SANTIMENT_KEY in .Renviron")


#### DIRECTORY SETUP ####
raw_dir   <- "data/raw"
clean_dir <- "data/clean"

dir.create(raw_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(clean_dir, showWarnings = FALSE, recursive = TRUE)

#### CRYPTO PRICES #######

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
      data.frame(type = "prices", payload$prices),
      data.frame(type = "market_caps", payload$market_caps),
      data.frame(type = "total_volumes", payload$total_volumes)
    )
    names(raw_combined)[2:3] <- c("timestamp", "value")

    raw_combined <- raw_combined |>
      mutate(date = as.POSIXct(timestamp / 1000, origin = "1970-01-01", tz = "UTC"))

    write_csv(raw_combined, raw_path)
    message("Raw crypto file saved: ", raw_path)
  }

  # Clean
  if (status_code(res) == 200 && !is.null(payload)) {

    df_price <- as.data.frame(payload$prices) |> rename(timestamp = 1, price = 2)
    df_vol   <- as.data.frame(payload$total_volumes) |> rename(timestamp = 1, volume = 2)
    df_cap   <- as.data.frame(payload$market_caps) |> rename(timestamp = 1, market_cap = 2)

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

btc <- fetch_crypto_data("bitcoin")
eth <- fetch_crypto_data("ethereum")
crypto_all <- bind_rows(btc, eth)

write_csv(crypto_all, file.path(clean_dir, "crypto_prices_1year.csv"))


###### SENTIMENT DATA #######

get_santiment_sentiment <- function(slug, from_date, to_date, api_key = s_api_key) {

  url <- "https://api.santiment.net/graphql"

  query <- paste0('{
    getMetric(metric: "sentiment_volume_consumed_total") {
      timeseriesData(
        slug: "', slug, '"
        from: "', from_date, 'T00:00:00Z"
        to: "', to_date, 'T00:00:00Z"
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

  if (status_code(res) == 200) {
    raw_json <- fromJSON(content(res, "text"))
    raw_df <- as.data.frame(raw_json$data$getMetric$timeseriesData)

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
  } else {
    warning("Sentiment request failed for ", slug)
    return(NULL)
  }
}

btc_sent <- get_santiment_sentiment("bitcoin", "2024-10-17", "2025-10-17")
eth_sent <- get_santiment_sentiment("ethereum", "2024-10-17", "2025-10-17")
sentiment_all <- bind_rows(btc_sent, eth_sent)


############ Google Trends ##########

fetch_daily_trends <- function(keyword, start_date = "2024-10-17", end_date = "2025-10-17", geo = "US") {

  save_raw  <- file.path(raw_dir)
  save_clean <- file.path(clean_dir)

  dates <- seq(as.Date(start_date), as.Date(end_date), by = "90 days")
  ranges <- data.frame(
    start = head(dates, -1),
    end = tail(dates, -1) - 1
  )

  trend_list <- map(1:nrow(ranges), function(i) {
    range_str <- paste(ranges$start[i], ranges$end[i])
    tryCatch(
      gtrends(keyword = keyword, geo = geo, time = range_str)$interest_over_time,
      error = function(e) NULL
    )
  })

  all_data <- bind_rows(trend_list)

  if (nrow(all_data) == 0) return(NULL)

  if ("date" %in% names(all_data)) {
    all_data <- rename(all_data, datetime = date)
  } else if ("time" %in% names(all_data)) {
    all_data <- rename(all_data, datetime = time)
  }

  final <- all_data |>
    mutate(
      date = as.Date(datetime),
      search_interest = as.numeric(replace(hits, hits == "<1", 0)),
      coin = toupper(keyword)
    ) |>
    select(date, coin, search_interest)

  # save
  write_csv(all_data, file.path(save_raw, paste0("google_trends_raw_", toupper(keyword), "_", Sys.Date(), ".csv")))
  write_csv(final, file.path(save_clean, paste0("google_trends_clean_", toupper(keyword), "_", Sys.Date(), ".csv")))

  return(final)
}

btc_trends <- fetch_daily_trends("bitcoin")
eth_trends <- fetch_daily_trends("ethereum")

trends_all <- bind_rows(btc_trends, eth_trends)


############ MERGE ALL DATA ############

merged <- crypto_all |>
  mutate(date = as.Date(date)) |>
  left_join(sentiment_all, by = c("date", "coin")) |>
  left_join(trends_all, by = c("date", "coin")) |>
  group_by(coin) |>
  arrange(date) |>
  mutate(
    daily_return = price / lag(price) - 1,
    volatility_7d = rollapply(daily_return, 7, sd, fill = NA, align = "right")
  )

write_csv(merged, file.path(clean_dir, "merged_crypto_sentiment_trends.csv"))
