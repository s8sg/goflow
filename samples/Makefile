all: setup compile run
clean:
	@rm flow
setup:
	@sudo docker run -p 6379:6379 -d redis

compile:
	@go build -o flow
run: 
	./flow &
check:
	@curl -d "Hell World" localhost:8080/myflow
	@echo "\n"
shutdown:
	@docker ps | awk -F ' ' '{ print $1 }' | tail -n 1 | xargs docker stop