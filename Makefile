setup:
	python -m venv venv
	source venv/bin/activate
	pip install -r requeriments

utils:
	pip freeze > requirements.txt
