# Serve my localhost to thw eorld through authentication

The easiest way to share a local service with a password is to use ngrok. It creates a temporary public URL that forwards to a local port. Ngrok supports HTTP basic authentication: you can require a username and password when someone opens the link. The steps are:

1. Sign up and install ngrok:

Create a free account at ngrok.com, download the CLI for your OS and run the command shown in the dashboard to save your authtoken (e.g. ./ngrok authtoken <your_token>).

Instructions here:
- https://dashboard.ngrok.com/get-started/setup/windows
- https://github.com/akasranjan005/ngrok-cheatsheet/blob/master/README.md

2. Open a password‑protected tunnel for TerriaMap
With TerriaMap running locally on port 3001, start ngrok like this:
`ngrok http --basic-auth="myuser:mypassword" 3001`

The `--basic-auth="user:password"` flag tells ngrok to enforce HTTP basic authentication on the generated URL.
Ngrok will output a forwarding URL such as https://abcd1234.ngrok.io. When someone visits that URL, their browser will prompt for the username (`myuser`) and password (`mypassword`). After entering the credentials, they will be proxied to http://localhost:3001/ and will see your TerriaMap site.

3. Open a password‑protected tunnel for datacube‑ows
Start a second tunnel for the WMS service:
`ngrok http --basic-auth="myuser:mypassword" 8080`

This will produce another public URL (e.g. `https://wxyz5678.ngrok.io`). Your colleague can open `https://wxyz5678.ngrok.io/?service=WMS&request=GetCapabilities` and supply the same credentials to access the WMS capabilities document.

A free ngrok account allows multiple tunnels at once, but sessions are limited (8‑hour lifetime) and the subdomain changes each time. Paid plans let you pick a fixed sub‑domain. You can also create a single ngrok configuration file (~/.ngrok2/ngrok.yml) to start both tunnels at once.


# Run it
https://stackoverflow.com/questions/70760748/cant-run-multiple-ngrok-tunnels-using-config-file


`cd .\ngrok`
`ngrok start --all --config ngrok.yml`