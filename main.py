from jpod import queries

ai_jobs = queries.get_ai_postings_by_region(regional_level=2, jpod_version="test")
total_jobs = queries.get_total_postings_by_region(regional_level=2, jpod_version="test") # => this gives way too many regional observations!
