# piped-backend build automation (Pi). The YouTube extractor is a local fork
# (~/NewPipeExtractor, ~12 patches) compiled to a jar that drops into libs/, then
# the backend image is rebuilt. No JDK on the host -> the jar builds inside a
# temurin container. Manual multi-step before; now `make deploy`.
#
#   make jar      - compile the NewPipe fork -> libs/NewPipeExtractor-patched.jar
#   make backend  - rebuild the piped-backend image (gradle shadowJar)
#   make restart  - recreate the running container with the fresh image
#   make deploy   - jar + backend + restart (the full chain)
#   make logs     - follow backend logs

NEWPIPE_DIR ?= $(HOME)/NewPipeExtractor
COMPOSE_DIR ?= $(HOME)/piped
LIBS_JAR    := libs/NewPipeExtractor-patched.jar

.PHONY: deploy jar backend restart logs

deploy: jar backend restart

jar:
	@mkdir -p $(HOME)/.gradle-docker-cache
	cd $(NEWPIPE_DIR) && docker run --rm -v "$$PWD":/app -w /app -v $(HOME)/.gradle-docker-cache:/root/.gradle eclipse-temurin:21-jdk \
		bash -c "apt-get update -qq >/dev/null && apt-get install -y -qq git >/dev/null && \
		         git config --global --add safe.directory /app && \
		         ./gradlew :extractor:jar -x test --console=plain --no-daemon --no-configuration-cache"
	@cp $(LIBS_JAR) $(LIBS_JAR).bak-$$(date +%Y%m%d-%H%M) 2>/dev/null || true
	cp $$(ls -t $(NEWPIPE_DIR)/extractor/build/libs/extractor-v*.jar | head -1) $(LIBS_JAR)
	@echo "-> $(LIBS_JAR) updated ($$(stat -c %s $(LIBS_JAR)) bytes)"

backend:
	docker build -t piped-local:latest .

restart:
	cd $(COMPOSE_DIR) && docker compose up -d piped-backend

logs:
	docker logs piped-backend --since 2m -f
