# 🔑 Snowpark WIDF Lambda Loader

**Keyless ETL: AWS Lambda → Snowflake**

Demonstrates Snowflake [Workload Identity Federation (WIDF)](https://docs.snowflake.com/en/user-guide/workload-identity-federation) with AWS Lambda. No passwords, no secrets, no key pairs - just IAM trust!

## Overview

```
┌─────────────────┐     WIDF Trust     ┌─────────────────┐
│   AWS Lambda    │ ◄────────────────► │    Snowflake    │
│                 │                     │                 │
│  IAM Role ARN   │    No Secrets!     │  SERVICE User   │
│  (identity)     │    No Passwords!   │  (trusts ARN)   │
└─────────────────┘                     └─────────────────┘
        │
        │ S3 Trigger
        ▼
┌─────────────────┐
│    S3 Bucket    │
│  incoming/*.json│
└─────────────────┘
```

## Prerequisites

- **Docker** - Required for SAM build (`--use-container`)
  - [Install Docker Desktop](https://www.docker.com/products/docker-desktop/)
- **AWS CLI** - Configured with credentials
  - [Install AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
- **AWS SAM CLI** - For Lambda deployment
  - [Install SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)
- **Snowflake CLI** - For running SQL scripts
  - [Install Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/installation/installation)
- **Task** - Task runner
  - [Install Task](https://taskfile.dev/installation/)
- **uv** - Python package manager
  - [Install uv](https://docs.astral.sh/uv/getting-started/installation/)

## Quick Start

```bash
# 1. Copy and configure environment
cp env.example .env
# Edit .env with your AWS and Snowflake settings

# 2. Verify configuration
task default

# 3. Check prerequisites
task aws:check-sam

# 4. Deploy (see DEMO.md for full walkthrough)
task aws:deploy
```

## Demo Walkthrough

See [DEMO.md](DEMO.md) for the full live demo with two parts:

1. **Part I** - Deploy Lambda without WIDF → Watch it fail
2. **Part II** - Configure WIDF → Watch it succeed

## Project Structure

```
├── app.py                 # Lambda handler (Snowpark + WIDF)
├── test_app.py            # Integration tests
├── Taskfile.yml           # All automation tasks
├── DEMO.md                # Live demo guide
├── aws/
│   └── template.yaml      # SAM template (Lambda, S3, IAM)
├── scripts/
│   ├── setup.sql          # Snowflake DB/schema setup
│   └── lambda-wid.sql     # WIDF service user creation
└── data/
    └── sample-data.json   # Test data
```

## Key Commands

| Command | Description |
|---------|-------------|
| `task default` | Show configuration |
| `task aws:deploy` | Deploy Lambda + S3 |
| `task snow:lambda-wid` | Create WIDF service user |
| `task aws:test` | Upload test data |
| `task aws:logs` | Tail CloudWatch logs |
| `task snow:query` | Query loaded data |

## The Magic ✨

```python
connection_params = {
    "account": "...",
    "authenticator": "WORKLOAD_IDENTITY",  # 🔑 THE MAGIC!
    # NO password, NO secret key, NO key pair
}
```

## License

See [LICENSE](LICENSE)
