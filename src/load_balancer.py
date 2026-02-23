"""
Load balancer configuration and statistics manager (HAProxy-inspired).
Manages frontends, backends, ACLs, and generates real HAProxy configurations.
"""

import sqlite3
import json
import os
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from enum import Enum
from datetime import datetime
from collections import defaultdict


class Algorithm(str, Enum):
    ROUNDROBIN = "roundrobin"
    LEASTCONN = "leastconn"
    FIRST = "first"
    RANDOM = "random"
    SOURCE = "source"


class ServerStatus(str, Enum):
    UP = "up"
    DOWN = "down"
    DRAIN = "drain"
    MAINT = "maint"


class Mode(str, Enum):
    HTTP = "http"
    TCP = "tcp"


@dataclass
class BackendServer:
    """Individual server in a backend."""
    id: str
    backend_id: str
    name: str
    address: str
    port: int
    weight: int = 1
    check: bool = True
    max_conn: int = 100
    backup: bool = False
    status: ServerStatus = ServerStatus.UP
    current_sessions: int = 0
    bytes_in: int = 0
    bytes_out: int = 0
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "backend_id": self.backend_id,
            "name": self.name,
            "address": self.address,
            "port": self.port,
            "weight": self.weight,
            "check": self.check,
            "max_conn": self.max_conn,
            "backup": self.backup,
            "status": self.status.value,
            "current_sessions": self.current_sessions,
            "bytes_in": self.bytes_in,
            "bytes_out": self.bytes_out,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class Backend:
    """Backend server pool."""
    id: str
    name: str
    algorithm: Algorithm = Algorithm.ROUNDROBIN
    servers: List[BackendServer] = field(default_factory=list)
    health_check: str = "/health"
    cookie_name: Optional[str] = None
    mode: Mode = Mode.HTTP
    timeout_connect: int = 5000
    timeout_server: int = 30000
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "algorithm": self.algorithm.value,
            "servers": [asdict(s) for s in self.servers],
            "health_check": self.health_check,
            "cookie_name": self.cookie_name,
            "mode": self.mode.value,
            "timeout_connect": self.timeout_connect,
            "timeout_server": self.timeout_server,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class ACLRule:
    """ACL rule for frontend."""
    name: str
    condition: str  # e.g., "hdr(host) -i worlds.blackroad.io"
    use_backend: str  # backend id to route to


@dataclass
class Frontend:
    """Frontend listener."""
    id: str
    name: str
    bind_address: str
    bind_port: int
    ssl: bool = False
    default_backend: Optional[str] = None
    acl_rules: List[ACLRule] = field(default_factory=list)
    mode: Mode = Mode.HTTP
    timeout_connect: int = 5000
    timeout_client: int = 50000
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "bind_address": self.bind_address,
            "bind_port": self.bind_port,
            "ssl": self.ssl,
            "default_backend": self.default_backend,
            "acl_rules": [asdict(a) for a in self.acl_rules],
            "mode": self.mode.value,
            "timeout_connect": self.timeout_connect,
            "timeout_client": self.timeout_client,
            "created_at": self.created_at.isoformat(),
        }


class LoadBalancer:
    """HAProxy-inspired load balancer manager."""

    def __init__(self, db_path: Optional[str] = None):
        """Initialize LoadBalancer with SQLite database."""
        if db_path is None:
            db_path = os.path.expanduser("~/.blackroad/haproxy.db")
        
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        """Initialize SQLite database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS frontends (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    config TEXT NOT NULL,
                    created_at TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS backends (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    config TEXT NOT NULL,
                    created_at TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS servers (
                    id TEXT PRIMARY KEY,
                    backend_id TEXT NOT NULL,
                    config TEXT NOT NULL,
                    created_at TIMESTAMP,
                    FOREIGN KEY(backend_id) REFERENCES backends(id)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS acl_rules (
                    id TEXT PRIMARY KEY,
                    frontend_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    condition TEXT NOT NULL,
                    use_backend TEXT NOT NULL,
                    FOREIGN KEY(frontend_id) REFERENCES frontends(id)
                )
            """)
            conn.commit()

    def add_frontend(self, name: str, bind_address: str, bind_port: int,
                    ssl: bool = False, mode: str = "http") -> str:
        """Add a frontend listener."""
        import uuid
        
        frontend_id = f"frontend_{uuid.uuid4().hex[:8]}"
        mode_enum = Mode[mode.upper()]
        
        frontend = Frontend(
            id=frontend_id,
            name=name,
            bind_address=bind_address,
            bind_port=bind_port,
            ssl=ssl,
            mode=mode_enum,
        )
        
        config_json = json.dumps(frontend.to_dict())
        now = datetime.now().isoformat()
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO frontends (id, name, config, created_at) VALUES (?, ?, ?, ?)",
                (frontend_id, name, config_json, now)
            )
            conn.commit()
        
        return frontend_id

    def set_default_backend(self, frontend_id: str, backend_id: str) -> bool:
        """Set default backend for a frontend."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT config FROM frontends WHERE id = ?", (frontend_id,))
            row = cursor.fetchone()
            if not row:
                return False
            
            frontend_dict = json.loads(row[0])
            frontend_dict["default_backend"] = backend_id
            
            conn.execute(
                "UPDATE frontends SET config = ? WHERE id = ?",
                (json.dumps(frontend_dict), frontend_id)
            )
            conn.commit()
        return True

    def add_acl(self, frontend_id: str, name: str, condition: str, use_backend: str) -> bool:
        """Add ACL rule to frontend."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT config FROM frontends WHERE id = ?", (frontend_id,))
            row = cursor.fetchone()
            if not row:
                return False
            
            frontend_dict = json.loads(row[0])
            frontend_dict["acl_rules"].append({
                "name": name,
                "condition": condition,
                "use_backend": use_backend,
            })
            
            conn.execute(
                "UPDATE frontends SET config = ? WHERE id = ?",
                (json.dumps(frontend_dict), frontend_id)
            )
            conn.commit()
        return True

    def add_backend(self, name: str, algorithm: str = "roundrobin",
                   mode: str = "http", health_path: str = "/health") -> str:
        """Add a backend pool."""
        import uuid
        
        backend_id = f"backend_{uuid.uuid4().hex[:8]}"
        algo_enum = Algorithm[algorithm.upper()]
        mode_enum = Mode[mode.upper()]
        
        backend = Backend(
            id=backend_id,
            name=name,
            algorithm=algo_enum,
            mode=mode_enum,
            health_check=health_path,
        )
        
        config_json = json.dumps(backend.to_dict())
        now = datetime.now().isoformat()
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO backends (id, name, config, created_at) VALUES (?, ?, ?, ?)",
                (backend_id, name, config_json, now)
            )
            conn.commit()
        
        return backend_id

    def add_server(self, backend_id: str, name: str, address: str, port: int,
                  weight: int = 1, check: bool = True, max_conn: int = 100) -> str:
        """Add server to backend."""
        import uuid
        
        server_id = f"server_{uuid.uuid4().hex[:8]}"
        
        server = BackendServer(
            id=server_id,
            backend_id=backend_id,
            name=name,
            address=address,
            port=port,
            weight=weight,
            check=check,
            max_conn=max_conn,
        )
        
        config_json = json.dumps(server.to_dict())
        now = datetime.now().isoformat()
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO servers (id, backend_id, config, created_at) VALUES (?, ?, ?, ?)",
                (server_id, backend_id, config_json, now)
            )
            conn.commit()
        
        return server_id

    def set_server_status(self, server_id: str, status: str) -> bool:
        """Set server status (up/down/drain/maint)."""
        try:
            status_enum = ServerStatus[status.upper()]
        except KeyError:
            return False
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT config FROM servers WHERE id = ?", (server_id,))
            row = cursor.fetchone()
            if not row:
                return False
            
            server_dict = json.loads(row[0])
            server_dict["status"] = status
            
            conn.execute(
                "UPDATE servers SET config = ? WHERE id = ?",
                (json.dumps(server_dict), server_id)
            )
            conn.commit()
        return True

    def generate_haproxy_cfg(self) -> str:
        """Generate HAProxy configuration syntax."""
        config_lines = [
            "global",
            "    log stdout local0",
            "    log stdout local1 notice",
            "    chroot /var/lib/haproxy",
            "    stats socket /run/haproxy/admin.sock mode 660 level admin",
            "",
        ]
        
        with sqlite3.connect(self.db_path) as conn:
            # Frontends
            cursor = conn.execute("SELECT config FROM frontends")
            for (config,) in cursor.fetchall():
                frontend_dict = json.loads(config)
                config_lines.append(f"frontend {frontend_dict['name']}")
                config_lines.append(f"    bind {frontend_dict['bind_address']}:{frontend_dict['bind_port']}")
                config_lines.append(f"    mode {frontend_dict['mode']}")
                
                # ACL rules
                for acl in frontend_dict.get("acl_rules", []):
                    config_lines.append(f"    acl {acl['name']} {acl['condition']}")
                    config_lines.append(f"    use_backend {acl['use_backend']} if {acl['name']}")
                
                if frontend_dict.get("default_backend"):
                    config_lines.append(f"    default_backend {frontend_dict['default_backend']}")
                config_lines.append("")
            
            # Backends
            cursor = conn.execute("SELECT config FROM backends")
            for (config,) in cursor.fetchall():
                backend_dict = json.loads(config)
                config_lines.append(f"backend {backend_dict['name']}")
                config_lines.append(f"    balance {backend_dict['algorithm']}")
                config_lines.append(f"    mode {backend_dict['mode']}")
                
                # Servers in backend
                srv_cursor = conn.execute(
                    "SELECT config FROM servers WHERE backend_id = ?",
                    (backend_dict['id'],)
                )
                for (srv_config,) in srv_cursor.fetchall():
                    srv_dict = json.loads(srv_config)
                    weight_str = f" weight {srv_dict['weight']}" if srv_dict['weight'] != 1 else ""
                    check_str = " check" if srv_dict['check'] else ""
                    config_lines.append(
                        f"    server {srv_dict['name']} {srv_dict['address']}:{srv_dict['port']}{weight_str}{check_str}"
                    )
                config_lines.append("")
        
        return "\n".join(config_lines)

    def get_stats(self, backend_id: Optional[str] = None) -> dict:
        """Get statistics for backends and servers."""
        stats = {"backends": {}}
        
        with sqlite3.connect(self.db_path) as conn:
            if backend_id:
                backends = [backend_id]
            else:
                cursor = conn.execute("SELECT id FROM backends")
                backends = [row[0] for row in cursor.fetchall()]
            
            for bid in backends:
                cursor = conn.execute("SELECT config FROM backends WHERE id = ?", (bid,))
                row = cursor.fetchone()
                if not row:
                    continue
                
                backend_dict = json.loads(row[0])
                backend_stats = {
                    "name": backend_dict["name"],
                    "servers": {},
                    "total_sessions": 0,
                    "total_bytes_in": 0,
                    "total_bytes_out": 0,
                }
                
                srv_cursor = conn.execute(
                    "SELECT config FROM servers WHERE backend_id = ?",
                    (bid,)
                )
                for (srv_config,) in srv_cursor.fetchall():
                    srv_dict = json.loads(srv_config)
                    backend_stats["servers"][srv_dict["name"]] = {
                        "status": srv_dict["status"],
                        "sessions": srv_dict["current_sessions"],
                        "bytes_in": srv_dict["bytes_in"],
                        "bytes_out": srv_dict["bytes_out"],
                    }
                    backend_stats["total_sessions"] += srv_dict["current_sessions"]
                    backend_stats["total_bytes_in"] += srv_dict["bytes_in"]
                    backend_stats["total_bytes_out"] += srv_dict["bytes_out"]
                
                stats["backends"][bid] = backend_stats
        
        return stats

    def simulate_request(self, src_ip: str, host: str, path: str) -> dict:
        """Simulate request routing through ACLs to backend and server."""
        result = {
            "src_ip": src_ip,
            "host": host,
            "path": path,
            "routed_to_frontend": None,
            "routed_to_backend": None,
            "routed_to_server": None,
        }
        
        with sqlite3.connect(self.db_path) as conn:
            # Find frontend (simplified - first matching)
            cursor = conn.execute("SELECT id, config FROM frontends LIMIT 1")
            row = cursor.fetchone()
            if not row:
                return result
            
            frontend_id, frontend_config = row
            frontend_dict = json.loads(frontend_config)
            result["routed_to_frontend"] = frontend_id
            
            # Check ACLs (simplified matching)
            backend_id = frontend_dict.get("default_backend")
            for acl in frontend_dict.get("acl_rules", []):
                if host in acl.get("condition", ""):
                    backend_id = acl["use_backend"]
                    break
            
            result["routed_to_backend"] = backend_id
            
            if backend_id:
                # Pick server (round-robin simplified)
                srv_cursor = conn.execute(
                    "SELECT config FROM servers WHERE backend_id = ? AND json_extract(config, '$.status') = 'up' LIMIT 1",
                    (backend_id,)
                )
                srv_row = srv_cursor.fetchone()
                if srv_row:
                    srv_dict = json.loads(srv_row[0])
                    result["routed_to_server"] = srv_dict["name"]
        
        return result

    def get_server_weights(self, backend_id: str) -> dict:
        """Get effective weights for servers after health check."""
        weights = {}
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT config FROM servers WHERE backend_id = ?",
                (backend_id,)
            )
            
            for (config,) in cursor.fetchall():
                srv_dict = json.loads(config)
                # Only count UP servers
                if srv_dict["status"] == "up":
                    weights[srv_dict["name"]] = srv_dict["weight"]
        
        return weights

    def list_backends(self) -> List[dict]:
        """List all backends."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT id, name, created_at FROM backends")
            return [{"id": row[0], "name": row[1], "created_at": row[2]} for row in cursor.fetchall()]

    def list_frontends(self) -> List[dict]:
        """List all frontends."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT id, name, created_at FROM frontends")
            return [{"id": row[0], "name": row[1], "created_at": row[2]} for row in cursor.fetchall()]


if __name__ == "__main__":
    import sys
    
    lb = LoadBalancer()
    
    if len(sys.argv) < 2:
        print("Usage: load_balancer.py [backends|frontends|add-server|generate-config|stats]")
        sys.exit(1)
    
    cmd = sys.argv[1]
    
    if cmd == "backends":
        backends = lb.list_backends()
        for b in backends:
            print(f"  {b['id']}: {b['name']}")
    elif cmd == "frontends":
        frontends = lb.list_frontends()
        for f in frontends:
            print(f"  {f['id']}: {f['name']}")
    elif cmd == "generate-config":
        conf = lb.generate_haproxy_cfg()
        print(conf)
    elif cmd == "stats" and len(sys.argv) >= 3:
        backend_id = sys.argv[2]
        stats = lb.get_stats(backend_id)
        print(json.dumps(stats, indent=2))
    elif cmd == "add-server" and len(sys.argv) >= 5:
        backend_id = sys.argv[2]
        name = sys.argv[3]
        address = sys.argv[4]
        port = int(sys.argv[5]) if len(sys.argv) > 5 else 80
        weight = int(sys.argv[7]) if len(sys.argv) > 6 and sys.argv[6] == "--weight" else 1
        sid = lb.add_server(backend_id, name, address, port, weight)
        print(f"Created server: {sid}")
