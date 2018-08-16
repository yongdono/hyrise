cat scripts/supported_job_queries.txt | parallel -j $2 --eta --results $3 $1 {}
