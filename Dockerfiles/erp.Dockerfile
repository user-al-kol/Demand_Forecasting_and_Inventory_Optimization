FROM python:3.11-slim

WORKDIR /app

RUN mkdir erp_dumps

COPY ../ERP_Generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ../ERP_Generator/generate_erp_dump.py .

CMD ["python", "generate_erp_dump.py"]
