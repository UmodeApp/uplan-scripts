setup:
	python -m venv venv
	source venv/bin/activate
	pip install -r requirements.txt

active_venv_windows:
	venv\Scripts\Activate.ps1

utils:
	pip freeze > requirements.txt

- Criar schema do objeto de estoque para os itens, para mostrar para o Andre