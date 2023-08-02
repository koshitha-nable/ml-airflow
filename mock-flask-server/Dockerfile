FROM python:3.8.3-alpine

RUN pip install --no-cache-dir --upgrade pip

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python3","app.py" ]