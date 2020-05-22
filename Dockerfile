
FROM python:3.6-onbuild

EXPOSE 8000

CMD python main.py run_server
