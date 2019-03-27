; readClimo.ncl
;
;
; read climatological analysis files
;
; load other scripts
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/contrib/cd_string.ncl"

load "utilityFuncs.ncl"
load "addNewDim.ncl"

undef("readClimoMean")
function readClimoMean(									\\
sourcePath[1]		: string,	\\	; Root path of the ensemble files. Files should be in sourcePath/yyyy/yyyymm/
gribtype[1]			: string,	\\	; Type of file to read (master, pgrba, etc)
validTimes[*] 		: double,	\\	; Valid times in seconds since 01 Jan 1970 00:00 UTC
variableName[1]		: string 	\\	; Name of the desired variable in the WRF file
)
	local cpuStart, wallStart, nTimes, climoTimes, filenames, nFiles, missing, infiles, tempVariable, nDims, variable, levels, levelNames, \\
		cpuEnd, cpuTime, wallEnd, wallTime
	begin

	cpuStart = get_cpu_time()
	wallStart = stringtointeger( systemfunc("date +%s") )

	nTimes = dimsizes(validTimes)

	calendar = cd_calendar( validTimes, 0 )
	climoTimes = ( cd_inv_calendar( conform(validTimes, 1979, -1), floattointeger( calendar(:,1) ), floattointeger( calendar(:,2) ), \\
	              	floattointeger( calendar(:,3) ), floattointeger( calendar(:,4) ), calendar(:,5), "hours since 1800-1-1 00:00:00", 0 ) )

	sortedClimoTimes = removeDuplicatesAndSort(climoTimes)

	if( gribtype.eq."1.0deg" ) then
		filenames = sourcePath + "/1.0deg/cmean_1d.1979" + removeDuplicates( cd_string( validTimes, "%N%D") )
	else if ( gribType.eq."2.5deg" ) then
		filenames = sourcePath + "/1.0deg/pgrba_mean.1959" + cd_string( validTimes, "%N") + "15"
	end if end if

	nFiles = dimsizes(filenames)

	if( any( .not. fileexists( filenames ) ) ) then
		missing = ind( .not. fileexists( filenames ) )
		print("The following files were not found:")
		print( filenames(missing) )
		print("Exiting...")
		status_exit(2)
	end if

	; turn off warnings to prevent unidentified grid messages
	err = NhlGetErrorObjectId()
	setvalues err
		"errLevel" : "Fatal"          ; only report Fatal errors
	end setvalues

	infiles = addfiles(filenames+".grb2", "r")
	ListSetType(infiles, "cat")

	; turn warnings back on
	setvalues err
		"errLevel" : "Warning"
	end setvalues

	tempVariable = infiles[:]->$variableName$
	; pick out validTimesAlt
	nDims = dimsizes( dimsizes(tempVariable) )
	if(nDims.eq.4) then 
		variable = tempVariable( sortedClimoTimes@indexMapping, :, :, : )
		levelNames = getvardims(variable)
		; variable&$levelNames(0)$ = validTimes
		levels = variable&$levelNames(1)$
		if( levels@units .eq. "Pa" ) then
			newLevels = new( dimsizes(levels), integer )
			copy_VarAtts(levels, newLevels)
			newLevels = floattointeger( levels/100 )
			newLevels@units = "hPa"
			delete(variable&$levelNames(1)$)
			variable&$levelNames(1)$ = newLevels
			delete(newLevels)
		end if 	; level@units .eq. "Pa"
		delete(levelNames)
		delete(levels)
	else if(nDims.eq.3) then
		variable = tempVariable( sortedClimoTimes@indexMapping, :, : )
	end if end if

	if( isatt(variable, "forecast_time") ) then delete( variable@forecast_time ) end if
	if( isatt(variable, "forecast_time_units") ) then delete( variable@forecast_time_units ) end if

	cpuEnd = get_cpu_time()
	cpuTime = cpuEnd - cpuStart
	cpuTime@units = "seconds"

	wallEnd = stringtointeger( systemfunc("date +%s") )
	wallTime = wallEnd - wallStart
	wallTime@units = "seconds"

	print("=== " + variableName + " Climatological data for " + nTimes + " times loaded in " + cd_string( wallTime, "%H hr %M min %S sec" ) + " ===" )
	print("=== " + variableName + " CPU time used: " + cd_string( cpuTime, "%H hr %M min %S sec" ) + " ===" )

	; remove degenerate dimensions and return
	return rm_single_dims(variable)

	end 	; readClimoMean

undef("readClimoStdDev")
function readClimoStdDev(									\\
sourcePath[1]		: string,	\\	; Root path of the ensemble files. Files should be in sourcePath/yyyy/yyyymm/
gribtype[1]			: string,	\\	; Type of file to read (master, pgrba, etc)
validTimes[*] 		: double,	\\	; Valid times in seconds since 01 Jan 1970 00:00 UTC
variableName[1]		: string 	\\	; Name of the desired variable in the WRF file
)
	local cpuStart, wallStart, nTimes, climoTimes, filenames, nFiles, missing, infiles, tempVariable, nDims, variable, levels, levelNames, newLevels, \\
			cpuEnd, cpuTime, wallEnd, wallTime
	begin

	cpuStart = get_cpu_time()
	wallStart = stringtointeger( systemfunc("date +%s") )

	nTimes = dimsizes(validTimes)

	calendar = cd_calendar( validTimes, 0 )
	climoTimes = ( cd_inv_calendar( conform(validTimes, 1979, -1), floattointeger( calendar(:,1) ), floattointeger( calendar(:,2) ), \\
					floattointeger( calendar(:,3) ), floattointeger( calendar(:,4) ), calendar(:,5), "hours since 1800-1-1 00:00:00", 0 ) )
	sortedClimoTimes = removeDuplicatesAndSort(climoTimes)

	if( gribtype.eq."1.0deg" ) then
		filenames = sourcePath + "/1.0deg/cstdv_1d.1979" + removeDuplicates( cd_string( validTimes, "%N%D") )
	else if ( gribType.eq."2.5deg" ) then
		filenames = sourcePath + "/1.0deg/pgrba_stdv.1959" + cd_string( validTimes, "%N") + "15"
	end if end if

	nFiles = dimsizes(filenames)

	if( any( .not. fileexists( filenames ) ) ) then
		missing = ind( .not. fileexists( filenames ) )
		print("The following files were not found:")
		print( filenames(missing) )
		print("Exiting...")
		status_exit(2)
	end if

	; turn off warnings to prevent unidentified grid messages
	err = NhlGetErrorObjectId()
	setvalues err
		"errLevel" : "Fatal"          ; only report Fatal errors
	end setvalues

	infiles = addfiles(filenames+".grb", "r")
	ListSetType(infiles, "cat")

	; turn warnings back on
	setvalues err
		"errLevel" : "Warning"
	end setvalues

	tempVariable = infiles[:]->$variableName$
	nDims = dimsizes( dimsizes(tempVariable) )
	if(nDims.eq.4) then 
		variable = tempVariable( sortedClimoTimes@indexMapping, :, :, : )
		levelNames = getvardims(variable)
		; variable&$levelNames(0)$ = validTimes
		levels = variable&$levelNames(1)$
		if( levels@units .eq. "Pa" ) then
			newLevels = new( dimsizes(levels), integer )
			copy_VarAtts(levels, newLevels)
			newLevels = floattointeger( levels/100 )
			newLevels@units = "hPa"
			delete(variable&$levelNames(1)$)
			variable&$levelNames(1)$ = newLevels
			delete(newLevels)
		end if 	; level@units .eq. "Pa"
		delete(levelNames)
		delete(levels)
	else if(nDims.eq.3) then
		variable = tempVariable( sortedClimoTimes@indexMapping, :, : )
	end if end if

	if( isatt(variable, "forecast_time") ) then delete( variable@forecast_time ) end if
	if( isatt(variable, "forecast_time_units") ) then delete( variable@forecast_time_units ) end if

	cpuEnd = get_cpu_time()
	cpuTime = cpuEnd - cpuStart
	cpuTime@units = "seconds"

	wallEnd = stringtointeger( systemfunc("date +%s") )
	wallTime = wallEnd - wallStart
	wallTime@units = "seconds"

	print("=== " + variableName + " Climatological data for " + nTimes + " times loaded in " + cd_string( wallTime, "%H hr %M min %S sec" ) + " ===" )
	print("=== " + variableName + " CPU time used: " + cd_string( cpuTime, "%H hr %M min %S sec" ) + " ===" )

	; remove degenerate dimensions and return
	return rm_single_dims(variable)

	end 	; readClimoStdDev

;
; Function to test readClimoMean
;
undef("test_readClimoMean")
procedure test_readClimoMean()
	local sourcePath, initYear, initMonth, initDay, initHour, t2, u
	begin

	sourcePath = "/scratch4/NCEPDEV/ensemble/noscrub/Walter.Kolczynski/climo"
	initYear = 2013
	initMonth = 12
	initDay = 30
	initHour = 0
	nTimes = 9
	stepDays = 73

	startTime = cd_inv_calendar( initYear, initMonth, initDay, initHour, 0, 0, "seconds since 1970-1-1 00:00:00", 0 )
	endTime = startTime + (nTimes - 1) * 86400 * stepDays
	; nTimes = doubletointeger( (endTime - startTime) / ( 86400 * stepDays) ) + 1

	initTimes = fspan( startTime, endTime, nTimes )
	copy_VarAtts( startTime, initTimes )

	uMean = readClimoMean( sourcePath, "1.0deg", initTimes, "UGRD_P8_L100_GLL0" )
	uStdDev = readClimoStdDev( sourcePath, "1.0deg", initTimes, "UGRD_P8_L100_GLL0" )

	printVarSummary(uMean)
	printVarSummary(uStdDev)

	print( cd_string(uMean&initial_time0_hours, "%Y%N%D%H") )

	; do t=0, nTimes-1
	; 	print( "" + cd_string( initTimes(t), "%Y%N%D") )
	; 	a = readClimoMean(sourcePath, "pgrba", initTimes(0:t), 288, "U_GRD_3_HTGL")
	; 	s = dimsizes(a)
	; 	if(t.gt.1 .and. s(0).ne.t+1) then 
	; 		exit
	; 	end if
	; 	delete(a)
	; 	delete(s)
	; end do

	end ;test_readClimoMean