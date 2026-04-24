@echo off
cd /d C:\RMTPROJECTS\dataengineering\geospatial
call venv_geo\Scripts\activate
set PATH=%CD%\venv_geo\Lib\site-packages\osgeo;%PATH%
echo GDAL: && gdal_translate --version
python scripts\08_visualization.py
pause
