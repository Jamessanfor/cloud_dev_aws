# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Install any needed packages specified in requirements.txt
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Set environment variables
ENV AWS_DEFAULT_REGION=us-east-1

# Run consumer_hw3.py when the container launches
CMD ["python", "consumer_hw3.py", "--storage", "s3", "--queue-url", "https://sqs.us-east-1.amazonaws.com/265690602160/cs5260-requests"]



