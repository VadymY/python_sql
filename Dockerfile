FROM python
RUN pip install requests 
WORKDIR = /app
COPY . .

CMD ["python", "req_api_1.py"]

