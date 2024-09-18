FROM public.ecr.aws/lambda/python:3.12

WORKDIR /var/task/

RUN pip install poetry

COPY . .

RUN ls -R

RUN poetry config virtualenvs.create false \
  && poetry install --no-root --no-dev --no-interaction --no-ansi

CMD ["handler.lambda_handler"]