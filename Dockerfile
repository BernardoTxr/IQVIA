# Use an official Python runtime as a parent image
FROM python:3.11

# Set the working directory in the container
WORKDIR /home/bernardo_teixeira/Documents/pj/IQVIA

# Copy the current directory contents into the container at /app
COPY . /home/bernardo_teixeira/Documents/pj/IQVIA

# Install any needed packages specified
RUN pip install --no-cache-dir matplotlib numpy pandas