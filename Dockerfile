FROM python:latest
WORKDIR /src
COPY . . 
COPY src/app.py /src/app.py
RUN pip install -r requirements.txt
CMD ["python", "app.py"]
