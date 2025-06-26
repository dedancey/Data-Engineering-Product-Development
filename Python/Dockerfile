from python:3.12.3

# setting working directory inside container
WORKDIR /app

# copy th epy file and everythign else into the /app folder

COPY . /app

#installing dependencies
RUN pip install -r Requirements.txt 

# running the application
ENTRYPOINT ["python", "myFirstPython.py"]