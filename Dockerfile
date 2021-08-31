ARG PYTHON_TAG
FROM python:$PYTHON_TAG
#ENV DEBIAN_FRONTEND=noninteractive
#ENV STATIC_INDEX 1

# Set working directory
WORKDIR /app

RUN apt-get -y update \
        && apt-get -y install vim
		
COPY ./requirements.txt ./

# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

COPY ./app/ /app/
RUN mkdir /logs \
	&& chmod 777 /logs

CMD [ "python", "/app/main.py", "start" ]
