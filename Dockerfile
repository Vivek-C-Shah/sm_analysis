FROM python:3.12-slim

WORKDIR /app

COPY . /app

RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libatlas-base-dev \
    libffi-dev \
    && apt-get clean

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 7860

CMD ["python", "app.2.gradio.py"]