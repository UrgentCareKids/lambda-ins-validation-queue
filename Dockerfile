FROM python:latest
COPY requirements.txt .
WORKDIR /src
COPY . . 
COPY src/app.py /src/app.py
RUN pip install -r requirements.txt
CMD [ "app.handler" ]
