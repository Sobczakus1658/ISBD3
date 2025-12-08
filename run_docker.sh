docker run -d -p 8080:8080 \
  -v "$(pwd)/data":/data \
  -v "$(pwd)/metastore":/app/metastore \
  -v "$(pwd)/errors":/app/errors \
  -v "$(pwd)/results":/app/results \
  -v "$(pwd)/queries":/app/queries \
  --entrypoint /app/main \
  --name isbd-container \
  isbd:latest
