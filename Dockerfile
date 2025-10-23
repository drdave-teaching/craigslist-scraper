FROM python:3.11-slim
WORKDIR /app
COPY ml/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY ml/train_and_predict.py ./train_and_predict.py
CMD ["python", "train_and_predict.py"]
