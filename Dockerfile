# Use a small official Python base image
FROM python:3.11-slim

# Set working dir
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app source
COPY . .

# Optional: set entrypoint to gunicorn, adjust module path to your app
ENTRYPOINT ["gunicorn", "main:app", "--bind", "0.0.0.0:$PORT", "--workers", "2"]

# Expose the port Cloud Run expects
ENV PORT 8080
EXPOSE 8080

# Run the app
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "app:app", "--workers", "1", "--threads", "8"]
