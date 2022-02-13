while true; do
	python run_until.py \
		--host "localhost" \
		--port 9501 \
		--target 60 

	sleep 1800
done
