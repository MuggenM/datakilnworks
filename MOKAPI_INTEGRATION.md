# Mokapi Integration Guide

Data Kiln Works is now integrated with your Mokapi mock server for LDAP and SMTP testing.

## 🔗 Connection Status

- **Mokapi Container**: `mokapi-mokapi-1`
- **Network**: `datakilnworks_default`
- **Dashboard**: http://localhost:8081/

### Services Configured

✅ **SMTP** (Email)
- Host: `mokapi-mokapi-1:2525`
- From: `datakilnworks@example.com`
- Password: `d@tak!lnworks123`
- Status: Connected and working

✅ **LDAP** (Authentication)
- Host: `mokapi-mokapi-1:389`
- Base DN: `dc=mokapi,dc=io`
- Status: Port accessible

---

## 📧 SMTP Email Testing

### Configuration File
Location: `/workspace/warehouse/.metadata/email_config.json`

```json
{
    "smtp_server": "mokapi-mokapi-1",
    "smtp_port": 2525,
    "use_tls": false,
    "sender_email": "datakilnworks@example.com",
    "sender_password": "d@tak!lnworks123",
    "sender_name": "Data Kiln Works",
    "enabled": true
}
```

### Send Test Email via CLI

```bash
docker exec local-datakilnworks-studio python3 << 'EOF'
import sys
sys.path.insert(0, '/workspace')
from web.email_reports import send_email

send_email(
    to_email="test@example.com",
    subject="Test Email",
    body="<h2>Test from Data Kiln Works</h2>",
    cc=None,
    attachments=[]
)
print("Email sent! Check http://localhost:8081/")
EOF
```

### Available Mailboxes (in Mokapi)

| Email | Username | Password |
|-------|----------|----------|
| datakilnworks@example.com | datakilnworks | d@tak!lnworks123 |
| test@example.com | bob | secret |

---

## 🔐 LDAP Authentication Testing

### LDAP Schema
- **Base DN**: `dc=mokapi,dc=io`
- **User Search Filter**: `(uid={username})`

### Test Users

| Username | Password | Full Name |
|----------|----------|-----------|
| awilliams | foo123 | Alice Williams |
| bmiller | bar123 | Bob Miller |

### Test LDAP via API

```bash
# Test LDAP connection
curl -X POST http://localhost:8891/api/auth/test-ldap \
  -H "Content-Type: application/json" \
  -d '{
    "server_host": "mokapi-mokapi-1",
    "server_port": 389,
    "encryption": "none",
    "bind_dn": "",
    "bind_password": "",
    "user_search_base": "dc=mokapi,dc=io",
    "user_search_filter": "(uid={username})"
  }'
```

### Test Authentication

```bash
# Login as Alice Williams
curl -X POST http://localhost:8891/api/auth/ldap/login \
  -H "Content-Type: application/json" \
  -d '{
    "username": "awilliams",
    "password": "foo123"
  }'

# Login as Bob Miller  
curl -X POST http://localhost:8891/api/auth/ldap/login \
  -H "Content-Type: application/json" \
  -d '{
    "username": "bmiller",
    "password": "bar123"
  }'
```

---

## 📊 View Results in Mokapi Dashboard

Open http://localhost:8081/ to see:
- 📬 Emails sent via SMTP
- 🔍 LDAP queries and authentication attempts
- 📡 Kafka messages (if used)

---

## 🚀 Use Cases

### 1. Email Alerts for Data Quality
Set up alerts in Data Kiln Works that send emails when:
- Query execution fails
- Data freshness exceeds threshold
- Schema changes detected

### 2. Scheduled Dashboard Reports
Export dashboards as PDFs/Excel and email to stakeholders on a schedule.

### 3. LDAP-Based Access Control
Authenticate users via LDAP and assign roles based on group membership.

### 4. Testing Email Workflows
Test email notifications without sending to real addresses.

---

## 🔧 Troubleshooting

### Email not sending?
```bash
# Check SMTP connectivity
docker exec local-datakilnworks-studio python3 -c "
import smtplib
server = smtplib.SMTP('mokapi-mokapi-1', 2525)
server.ehlo()
server.quit()
print('SMTP OK')
"
```

### LDAP not connecting?
```bash
# Check LDAP port
docker exec local-datakilnworks-studio python3 -c "
import socket
sock = socket.create_connection(('mokapi-mokapi-1', 389), timeout=5)
sock.close()
print('LDAP OK')
"
```

### View Mokapi logs
```bash
docker logs mokapi-mokapi-1 --tail 50 -f
```

---

## 📁 Mokapi Configuration Files

Located in `/datadrive/mokapi/config/`:

- **smtp.yaml** - SMTP server and mailbox configuration
- **ldap.yaml** - LDAP server configuration  
- **users.ldif** - LDAP user directory
- **schema.ldif** - LDAP schema definitions

---

## 🔗 Related Documentation

- Data Kiln Works Email API: http://192.168.3.2:8891/api/docs#/Email
- Data Kiln Works Auth API: http://192.168.3.2:8891/api/docs#/Authentication
- Mokapi Documentation: https://mokapi.io/docs/

---

**Integration completed on**: 2026-09-21  
**Status**: ✅ All services connected and operational
