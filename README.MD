Just based code from https://github.com/marcinbojko/commvault_exporter
Update metrics related to storage like total/used/free of library disks (file /app/commvault_exporter-ngocnd1v3.py)

But deploy is a bit difference

1. Create Python virtual environtment
2. Pip install requirement.txt
3. Input credential account commvault api to file
`
    # host and uri
    REQUEST_URI = os.getenv("COMMVAULT_REQUEST_URI", "http://commvaultsrv.sample.com")
    if not REQUEST_URI.endswith("/"):
        REQUEST_URI += "/"
    # display variables
    REQUEST_HOSTNAME = (urllib.parse.urlparse(REQUEST_URI)).netloc
    # user
    REQUEST_USER = os.getenv("COMMVAULT_REQUEST_USER", "api")
    # password
    REQUEST_PASSWORD = os.getenv("COMMVAULT_REQUEST_PASSWORD", "api")
`
4. Run test with venv /home/sysadmin/python/venv..... commvault-exporter.py 
5. Create systemd services
