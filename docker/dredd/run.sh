docker run \
    -i \
    -t \
    --rm \
    --name lyapi-dredd \
    --net host \
    -v $(pwd)/apiary.apib:/app/apiary.apib \
    lyapi-dredd:latest
