web: uvicorn api_server:app --host 0.0.0.0 --port $PORT --workers 2
ml_pipeline: python ml_pipeline.py
discord: python discord_dispatcher.py
enrichment: python apify_scrapers.py
