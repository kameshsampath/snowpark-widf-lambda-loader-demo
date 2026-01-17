# Snowpark WIDF Lambda - Container Image
# Using container deployment to handle large dependencies (snowpark + pandas + pyarrow)
#
# Why container? The zip package exceeds Lambda's 262MB limit due to:
# - snowflake-snowpark-python (~150MB)
# - pandas + pyarrow (~100MB)
# Container images support up to 10GB!

# Target architecture: arm64 (Mac M1/M2/M3, Graviton) or x86_64 (Intel/AMD)
# Must match LAMBDA_ARCH in .env for Lambda to run correctly
ARG TARGET_ARCH=arm64
FROM --platform=linux/${TARGET_ARCH} public.ecr.aws/lambda/python:3.11

# Install dependencies (using pip for compatibility in Lambda container)
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy application code
COPY app.py ${LAMBDA_TASK_ROOT}/

# Lambda handler entrypoint
CMD ["app.lambda_handler"]
