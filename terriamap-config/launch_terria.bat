@echo off
echo Stopping existing container...
docker stop terria || echo No running container named 'terria'

echo Removing existing container...
docker rm terria || echo No container named 'terria' to remove

echo Removing unused containers, networks, volumes...
docker system prune -f

echo Starting TerriaMap...
docker run -d ^
  --name terria ^
  -p 3001:3001 ^
  --mount type=bind,source=%cd%\config.json,destination=/app/wwwroot/config.json ^
  --mount type=bind,source=%cd%\simple.json,destination=/app/wwwroot/init/simple.json ^
  --mount type=bind,source=%cd%\logo.png,destination=/app/wwwroot/logo.png ^
  ghcr.io/terriajs/terriamap

echo TerriaMap started on http://localhost:3001
pause
