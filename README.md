# karaoke-pdf-parser

### Build the container
docker build -t pdf-parser .

### Run with mounted volumes for logs and data
docker run -v $(pwd)/logs:/app/logs -v $(pwd)/data:/app/data -v $(pwd)/karaoke_list.pdf:/app/karaoke_list.pdf pdf-parser