if "%GAFFER_ROOT%"=="" (
	echo GAFFER_ROOT environment variable not set
	exit /B 1
)

if "%GAFFER_DEADLINE_PATH%" =="" (
	echo GAFFER_DEADLINE_PATH should be set to the path of the GafferDeadline installation
	exit /B 1
)

set HOME=%USERPROFILE%

set IECORE_FONT_PATHS=%GAFFER_ROOT%/fonts

set PYTHONHOME=%GAFFER_ROOT%
set PYTHONPATH=%GAFFER_ROOT%\python;%PYTHONPATH%
set PATH=%GAFFER_ROOT%\lib;%PATH%
set PATH=%GAFFER_ROOT%\bin;%PATH%

SET PYTHONPATH=%GAFFER_DEADLINE_PATH%\python;%PYTHONPATH%
SET DEADLINE_DEPENDENCY_SCRIPT_PATH=P:\command_scripts\gaffer_batch_dependency.py
