BUILD_PATH='./build'
DEP_PATH='./dep'
help:
	@echo "clean - remove all build, test, coverage and Python artifacts"
	@echo "lint - check style"
	@echo "test - run tests quickly with the default Python"
	@echo "coverage - check code coverage quickly with the default Python"
	@echo "build - package"

all: clean test deps build

clean: 
	rm -rf ${BUILD_PATH}
	find . -name '__pycache__' -exec rm -rf {} +
	rm -f .coverage
	rm -rf htmlcov

lint:
	flake8 --exit-zero

test:
	pipenv run pytest

coverage:
	coverage run -m pytest
	coverage report -m
	coverage html

deps:
	mkdir -p ${DEP_PATH}
	pipenv lock -r > requirements.txt
	pip3 install -r requirements.txt --target ${DEP_PATH}	
	cd ${DEP_PATH} && zip -9mrv libs.zip .
	rm -rf libs && rm requirements.txt

dev-deps:
	pipenv lock -rd > requirements.txt

build: clean
		mkdir ${BUILD_PATH}
		cp main.py ${BUILD_PATH}
		mkdir ${BUILD_PATH}/configs
		cp configs/config.yaml ${BUILD_PATH}/configs
		cp configs/log4j.properties ${BUILD_PATH}
		cp configs/logging.json ${BUILD_PATH}
		zip -9rv ${BUILD_PATH}/src.zip ./src
		mv ${DEP_PATH}/libs.zip ${BUILD_PATH}
		rm -r ${DEP_PATH}