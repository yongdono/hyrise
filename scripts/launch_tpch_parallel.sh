cat scripts/supported_tpch_queries.txt | parallel -j $2 --eta --results $3 $1 {}
