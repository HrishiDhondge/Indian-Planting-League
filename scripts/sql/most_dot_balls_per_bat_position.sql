-- List of players who played most dot balls at each batting position 
WITH dot_ball_counts AS (
    SELECT 
        a.batter, 
        COUNT(*) AS zero_run_balls, 
        b.batting_order, 
        a.team, 
        a.match_number
    FROM 
        first_table a
    JOIN 
        second_table b
    ON 
        a.batter = b.batsman AND a.match_number = b.match_number
    WHERE 
        a.runs_total = 0
    GROUP BY 
        a.batter, b.batting_order, a.team, a.match_number
),
aggregated AS (
    SELECT 
        batter,
        SUM(zero_run_balls) AS total_dot_balls,
        batting_order,
        team,
        ROW_NUMBER() OVER (PARTITION BY batting_order ORDER BY SUM(zero_run_balls) DESC) AS row_num
    FROM 
        dot_ball_counts
    GROUP BY 
        batter, batting_order, team
)

SELECT batter, total_dot_balls, batting_order, team
FROM aggregated
WHERE row_num <= {top_N}
ORDER BY batting_order;