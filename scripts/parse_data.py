#!/usr/bin/python3
'''
This script parses the ball-by-ball data provided in json format.
Stores it in three different tables as parquet format.
'''

# Import necessary libraries 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
from pyspark.sql.functions import explode, col, lit, concat_ws
from pyspark.sql.functions import array, monotonically_increasing_id, row_number
from pyspark.sql.window import Window

def defining_info_schema(json_file):
    '''This defines the info section schema for consistency'''
    event_schema = StructType([
            StructField("name", StringType(), True),
            StructField("match_number", IntegerType(), True),
            StructField("stage", StringType(), True) # Only for Eliminator, Qualifiers, & Final
        ])
    
    officials_schema = StructType([
            StructField("match_referees", ArrayType(StringType()), True),
            StructField("reserve_umpires", ArrayType(StringType()), True),
            StructField("tv_umpires", ArrayType(StringType()), True),
            StructField("umpires", ArrayType(StringType()), True)
        ])
    
    toss_schema = StructType([
            StructField("decision", StringType(), True),
            StructField("winner", StringType(), True)
        ])
    
    outcome_schema = StructType([
            StructField("result", StringType(), True),
            StructField("eliminator", StringType(), True)
        ])
    
    info_schema = StructType([
        StructField("city", StringType(), True),
        StructField("dates", ArrayType(StringType()), True),
        StructField("event", event_schema, True),
        StructField("season", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("player_of_match", ArrayType(StringType()), True),
        StructField("outcome", outcome_schema, True),
        StructField("teams", ArrayType(StringType()), True),
        StructField("toss", toss_schema, True),
        StructField("players", MapType(StringType(), ArrayType(StringType())), True),
        StructField("officials", officials_schema, True)
    ])
    
    info_wrap_schema = StructType([
        StructField("info", info_schema, nullable=True)
    ])

    df = spark.read.schema(info_wrap_schema).option("multiline", True).json(json_file)
    return df


def extract_all_info(df, source="info"):
    '''Extracts all info defined in info schema'''
    return df.select(
        lit(source).alias("source"),
        col(f"{source}.city").alias("city"),
        col(f"{source}.dates").alias("dates"),
        col(f"{source}.event.name").alias("event_name"),
        col(f"{source}.event.match_number").alias("match_number"),
        col(f"{source}.event.stage").alias("stage"),
        col(f"{source}.season").alias("season"),
        col(f"{source}.gender").alias("gender"),
        col(f"{source}.player_of_match").alias("player_of_match"),
        col(f"{source}.outcome.result").alias("result"),
        col(f"{source}.outcome.eliminator").alias("eliminator"),
        col(f"{source}.teams").alias("teams"),
        col(f"{source}.toss.decision").alias("toss_decision"),
        col(f"{source}.toss.winner").alias("toss_winner"),
        col(f"{source}.players").alias("players"),
        col(f"{source}.officials.match_referees").alias("match_referees"),
        col(f"{source}.officials.reserve_umpires").alias("reserve_umpires"),
        col(f"{source}.officials.tv_umpires").alias("tv_umpires"),
        col(f"{source}.officials.umpires").alias("umpires"),
        # concat_ws(",", col("info.officials.umpires")).alias("umpires_joined") 
    )


def defining_innings_schema(json_file):
    '''This defines the innings section schema for consistency'''
    review_schema = StructType([
        StructField("by", StringType(), nullable=True),
        StructField("umpire", StringType(), nullable=True),
        StructField("batter", StringType(), nullable=True),
        StructField("decision", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True)
    ])
    extras_schema = StructType([
        StructField("wides", IntegerType(), nullable=True),
        StructField("noballs", IntegerType(), nullable=True),
        StructField("legbyes", IntegerType(), nullable=True)
    ])
    
    runs_schema = StructType([
        StructField("batter", IntegerType(), nullable=True),
        StructField("extras", IntegerType(), nullable=True),
        StructField("total", IntegerType(), nullable=True)
    ])
    wickets_schema = ArrayType(
        StructType([
            StructField("player_out", StringType(), nullable=True),
            StructField("kind", StringType(), nullable=True)
        ])
    )
    deliveries_schema = ArrayType(
        StructType([
            StructField("batter", StringType(), nullable=True),
            StructField("bowler", StringType(), nullable=True),
            StructField("non_striker", StringType(), nullable=True),
            StructField("runs", runs_schema, nullable=True),
            StructField("extras", extras_schema, nullable=True),
            StructField("wickets", wickets_schema, nullable=True),
            StructField("review", review_schema, nullable=True)
        ])
    )
    
    innings_schema = StructType([
        StructField("team", StringType(), nullable=True),
        StructField("overs", ArrayType(
            StructType([
                StructField("over", IntegerType(), nullable=True),
                StructField("deliveries", deliveries_schema, nullable=True)
            ])
        ), nullable=True)
    ])
        
    innings_top_schema = StructType([
        StructField("innings", ArrayType(innings_schema), nullable=True)
    ])

    return spark.read.schema(innings_top_schema).option("multiline", True).json(json_file)


def extract_all_innings(dataframe):
    '''Extracts all info defined in innings schema'''
    # Explode innings
    innings_df = dataframe.select(explode(col("innings")).alias("inning"))
    
    # Add team name
    innings_df = innings_df.withColumn("team", col("inning.team"))
    
    # Explode overs
    overs_df = innings_df.select(
        "team",
        explode(col("inning.overs")).alias("over")
    )
    
    # Add over number
    overs_df = overs_df.withColumn("over_number", col("over.over"))
    
    # Explode deliveries
    deliveries_df = overs_df.select(
        "team", "over_number",
        explode(col("over.deliveries")).alias("delivery")
    )
    
    # Flatten fields from df
    flat_df = deliveries_df.select(
        "team",
        "over_number",
        col("delivery.batter").alias("batter"),
        col("delivery.bowler").alias("bowler"),
        col("delivery.non_striker").alias("non_striker"),
        col("delivery.runs.batter").alias("runs_batter"),
        col("delivery.runs.extras").alias("runs_extras"),
        col("delivery.runs.total").alias("runs_total"),
        col("delivery.extras.wides").alias("extras_wides"),
        col("delivery.extras.noballs").alias("extras_noballs"),
        col("delivery.extras.legbyes").alias("extras_legbyes"),
        col("delivery.review.by").alias("review_by"),
        col("delivery.review.umpire").alias("review_umpire"),
        col("delivery.review.batter").alias("review_batter"),
        col("delivery.review.decision").alias("review_decision"),
        col("delivery.review.type").alias("review_type"),
        col("delivery.wickets")[0]["kind"].alias("wicket_kind"),
        col("delivery.wickets")[0]["player_out"].alias("player_out")
    )

    # Adding ball numbers to df
    window_over = Window.partitionBy("team", "over_number").orderBy(monotonically_increasing_id())
    return flat_df.withColumn("ball_number", row_number().over(window_over))# - 1)


def batting_order(df):
    """
    Infers the batting order per team based on first appearance
    of batters and non-strikers in the deliveries DataFrame.

    Args:
        df (DataFrame): Flattened delivery-level DataFrame with 'team', 'batter', 'non_striker', and 'over_number'.

    Returns:
        DataFrame: Team-wise batting order with unique batters and their batting position.
    """
    # Create a single column containing both batter and non-striker, and explode
    batters_df = (
        df.select("team", "over_number", "ball_number", array("batter", "non_striker").alias("batsman_array"))
          .select("team", "over_number", "ball_number", explode("batsman_array").alias("batsman"))
    )

    # Add a unique row ID to preserve the first appearance order
    batters_df = batters_df.withColumn("row_id", monotonically_increasing_id())

    # Get the first appearance of each batsman within each team
    first_seen_window = Window.partitionBy("team", "batsman").orderBy("row_id")
    unique_batters = (
        batters_df.withColumn("rn", row_number().over(first_seen_window))
                  .filter("rn = 1")
    )

    # Assign batting order by row_id (i.e., appearance order)
    batting_order_window = Window.partitionBy("team").orderBy("row_id")
    result_df = (
        unique_batters.withColumn("batting_order", row_number().over(batting_order_window))
                      .select("team", "batsman", "batting_order", "over_number", "ball_number")
    )

    return result_df


def add_two_id_columns(first_df, second_df, third_df):
    # Extract the first row & Get values
    first_row = first_df.select("match_number", "season").first()
    match_number_val = first_row["match_number"]
    season_val = first_row["season"]
    
    # insert two new columns
    updated_sec_df = second_df.withColumn("match_number", lit(match_number_val)) \
                           .withColumn("season", lit(season_val))

    updated_third_df = third_df.withColumn("match_number", lit(match_number_val)) \
                           .withColumn("season", lit(season_val))

    return updated_sec_df, updated_third_df



# Start the session
spark = SparkSession.builder.appName('parseJSON').getOrCreate()

lines = [line.strip() for line in open("data/IPL_match_ids_2025.txt")]
# print(lines)
for match in lines:
    schema_info = defining_info_schema(f"./ipl_json/{match}.json")
    tmp_info = extract_all_info(schema_info)

    schema_innings = defining_innings_schema(f"./ipl_json/{match}.json")
    innings_df = extract_all_innings(schema_innings)

    batting_df = batting_order(innings_df)

    tmp_innings, tmp_batting = add_two_id_columns(tmp_info, innings_df, batting_df)

    if lines.index(match) == 0:
        total_info, total_innings, total_batting = tmp_info, tmp_innings, tmp_batting
        continue

    ## Append tmp dfs to total dfs
    tmp_info = tmp_info.select(total_info.columns)
    total_info = total_info.union(tmp_info)

    tmp_innings = tmp_innings.select(total_innings.columns)
    total_innings = total_innings.union(tmp_innings)

    tmp_batting = tmp_batting.select(total_batting.columns)
    total_batting = total_batting.union(tmp_batting)
    # break

total_info.write.mode("overwrite").parquet("./data/total_info.parquet")
total_innings.write.mode("overwrite").parquet("./data/total_innings.parquet")
total_batting.write.mode("overwrite").parquet("./data/total_batting.parquet")

spark.stop()
