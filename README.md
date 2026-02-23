# BlackRoad OS HAProxy Load Balancer

Load balancer configuration and statistics manager with HAProxy-inspired architecture.

## Features
- Frontend and backend configuration
- ACL-based routing rules
- Health checking and server status tracking
- Real HAProxy config generation
- Request simulation and weight management
- Session and bandwidth statistics

## Installation
```bash
pip install -r requirements.txt
```

## Usage
```bash
python src/load_balancer.py backends
python src/load_balancer.py add-server BACKEND_ID worlds-1 localhost 8787 --weight 10
python src/load_balancer.py generate-config
python src/load_balancer.py stats BACKEND_ID
```
