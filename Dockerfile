FROM python:latest
COPY requirements.txt ./requirements.txt
WORKDIR /src
COPY . . 
COPY src/app.py /src/app.py
RUN pip install -r requirements.txt
CMD ["python", "app.py"]
# 