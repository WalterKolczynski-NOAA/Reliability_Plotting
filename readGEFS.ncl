; readGEFS.ncl
;   
; Provides an array of functions to read in data from GEFS
;
; Dependencies:
;   NCL version 6.1.0 or higher
;
; Routines assume data is stored using the following naming convention:
;   sourcePath/gefs.YYYYMMDD/HH/GRIBTYPE/geMMM.tHHz.GRIBTYPE.grbfFF or
;	sourcePath/gefs.YYYYMMDD/HH/GRIBTYPE/geMMM.tHHz.GRIBTYPEfFF (only for GRIBTYPE master)
; 
; where GRIDTYPE is the grib type (e.g. pgrba, pgrbb, pgrb2a, etc.) and MMM is the member id (c00, p01, p02, etc.)
; [NB: this is the default directory structure of the model output]
; 
; Included:
; 
; function readGEFS(sourcePath, gribtype, members, initTimes, forecastLeadsHr, variableName)
; Use:
; 	Read in GEFS data from an entire GEFS at multiple initialization and/or lead times. Any variable that exists in the GEFS files shoudl be supported.
; Inputs:
; 	sourcePath[1]       : string,   \\  ; Root path of the ensemble files.
; 	gribtype[1]         : string,   \\  ; Type of file to read (master, pgrba, etc)
; 	members[*]          : string,   \\  ; List of ensemble members
; 	initTimes[*] 		: double,	\\	; Initialization times in seconds since 01 Jan 1970 00:00 UTC
; 	forecastLeadsHr[*]  : integer,  \\  ; Forecast lead times in hours
; 	variableName[1]     : string    \\  ; Name of the desired variable in the grib file
; Output:
;	
;	
; Notes:
;
; 
; Created by:   Walter C. Kolczynski (Walter.Kolczynski@noaa.gov); 
;

; load other scripts
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/contrib/cd_string.ncl"
load "utilityFuncs.ncl"

; procedure to create static variables when the script is loaded without collatoral variable assignments
undef("initReadGEFS")
procedure initReadGEFS()
	local hashdata, hashkeys
	begin
	; pseudo-hash table to map ensstat file suffix to variable name
	; keys (suffixes) must be in alphabetical order
	undef("VARIABLE_NAME_HASH")
	VARIABLE_NAME_HASH    =  (/ "frzr",     "CFRZR_2_SFC_ave6h",    \\
								"icep",     "CICEPL_2_SFC_ave6h",   \\
								"prcp",     "A_PCP_2_SFC_acc6h",    \\
								"prmsl",    "PRMSL_2_MSL",          \\
								"rain",     "CRAIN_2_SFC_ave6h",    \\
								"rh700",    "R_H_2_ISBL",           \\
								"snow",     "CSNOW_2_SFC_ave6h",    \\
								"t2m",      "TMP_2_HTGL",           \\
								"t850",     "TMP_2_ISBL",           \\
								"u10m",     "U_GRD_2_HTGL",         \\
								"u250",     "U_GRD_2_ISBL",         \\
								"u850",     "U_GRD_2_ISBL",         \\
								"v10m",     "V_GRD_2_HTGL",         \\
								"v250",     "V_GRD_2_ISBL",         \\
								"v850",     "V_GRD_2_ISBL",         \\
								"z1000",    "HGT_2_ISBL",           \\
								"z500",     "HGT_2_ISBL"            \\
							/)
	hashkeys = VARIABLE_NAME_HASH(0::2)
	hashdata = VARIABLE_NAME_HASH(1::2)
	VARIABLE_NAME_HASH := hashdata
	VARIABLE_NAME_HASH!0  = "keys"
	VARIABLE_NAME_HASH&keys = hashkeys
	delete(hashkeys)
	delete(hashdata)

	end ; initReadGEFS

; initialize static variables
initReadGEFS()
STRICT_FILECHECK = False

undef("printMem")
procedure printMem (msg)
	local rss,cmd
	begin
	cmd = "ps --no-headers  -o 'rss' -C ncl"
	rss = max(toint(systemfunc(cmd))) / 1024.
	print("NCL memory:    "+sprintf("%6.2f",rss)+"     "+msg)
end

undef("readGEFS")
function readGEFS(									\\
sourcePath[1]       : string,   \\  ; Root path of the ensemble files. Files should be in sourcePath/yyyy/yyyymm/
gribtype[1]         : string,   \\  ; Type of file to read (master, pgrba, etc)
members[*]          : string,   \\  ; List of ensemble members
initTimes[*] 		: double,	\\	; Initialization times in seconds since 01 Jan 1970 00:00 UTC
forecastLeadsHr[*]  : integer,  \\  ; Forecast lead times in hours
variableName[1]     : string    \\  ; Name of the desired variable in the grib file
)
	local cpuStart, wallStart, nMembers, nTimes, nLeads, suffix, initTimesLarge, forecastLeadsHrLarge, membersLarge, yyyymmdd, hh, filenames, missing, infiles, tempVariable, \\
			cpuEnd, cpuTime, wallEnd, wallTime, variable, tempDimNames, tempDimSizes, nDims, dimSizes
	begin

	printMem("Begin readGEFS for " + variableName)

	cpuStart = get_cpu_time()
	wallStart = stringtointeger( systemfunc("date +%s") )

	nMembers 	= dimsizes(members)
	nTimes 		= dimsizes(initTimes)
	nLeads 		= dimsizes(forecastLeadsHr)

	suffix = where(gribtype.eq."pgrba", ".grb", ".grb2")

	initTimesLarge 			= ndtooned( conform_dims( (/nTimes, nLeads, nMembers/), initTimes, 0 ) )
	copy_VarAtts(initTimes, initTimesLarge)
	forecastLeadsHrLarge 	= ndtooned( conform_dims( (/nTimes, nLeads, nMembers/), forecastLeadsHr, 1 ) )
	membersLarge 			= ndtooned( conform_dims( (/nTimes, nLeads, nMembers/), members, 2 ) )

	; set up some strings for filename
	yyyymmdd    = cd_string( initTimesLarge, "%Y%N%D")
	hh          = cd_string( initTimesLarge, "%H")

	if( gribtype.eq."master" ) then
		filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/ge" + membersLarge + ".t" + hh + "z." + gribtype + ".grbf" + sprinti("%0.2i", forecastLeadsHrLarge)
	else if (gribtype.eq."pgrb2ap5") then
		filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/ge" + membersLarge + ".t" + hh + "z.pgrb2a.0p50.f" + sprinti("%0.3i", forecastLeadsHrLarge)
	else if (gribtype.eq."pgrb2a1p0") then
		filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/ge" + membersLarge + ".t" + hh + "z.pgrb2a.1p00.f" + sprinti("%0.3i", forecastLeadsHrLarge)
	else
		filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/ge" + membersLarge + ".t" + hh + "z." + gribtype + "f" + sprinti("%0.2i", forecastLeadsHrLarge)
	end if end if end if
	nFiles = dimsizes(filenames)

	if( any( .not. fileexists( filenames ) ) ) then
		print( filenames + " " + where(fileexists( filenames ), "found", "not found" ) )

		missing = ind( .not. fileexists( filenames ) )
		; print("The following files were not found:")
		; print( filenames(missing) )
		print("Exiting...")
		status_exit(2)
	end if

	infiles = addfiles(filenames+suffix, "r")
	ListSetType(infiles, "join")

	do f=0, ListCount(infiles)-1
		if( .not. isfilevar( infiles[f], variableName ) ) then
			print( filenames(f) + " does not contain " + variableName )
		end if
	end do

	tempVariable = infiles[:]->$variableName$

	if(nFiles.eq.1) then
		variable = tempVariable
	else
		tempDimNames = getvardims(tempVariable)
		tempDimSizes = dimsizes(tempVariable)
		nDims =  dimsizes(tempDimSizes) + 2
		dimSizes = new(nDims, integer)

		dimSizes(0) = nTimes
		dimSizes(1) = nLeads
		dimSizes(2) = nMembers
		if(nDims.gt.3) then
			dimSizes(3:) = tempDimSizes(1:)
		end if

		; printVarSummary( tempVariable )
		; print( dimSizes )

		variable = reshape( tempVariable, dimSizes )
		copy_VarAtts(tempVariable, variable)

		variable!0 = "initialization_time"
		variable!1 = "lead_time_hr"
		variable!2 = "ensemble_member"

		variable&initialization_time 	= initTimes
		variable&lead_time_hr			= forecastLeadsHr
		variable&ensemble_member		= members

		do d=3, nDims-1
			dimName = tempDimNames(d-2)
			variable!d = dimName
			if( iscoord(tempVariable, dimName) ) then variable&$dimName$ = tempVariable&$dimName$ end if
		end do
	end if
	delete(tempVariable)

	if( isatt(variable, "forecast_time") ) then delete( variable@forecast_time ) end if
	if( isatt(variable, "forecast_time_units") ) then delete( variable@forecast_time_units ) end if
	if( isatt(variable, "initial_time") ) then delete( variable@initial_time ) end if

	cpuEnd = get_cpu_time()
	cpuTime = cpuEnd - cpuStart
	cpuTime@units = "seconds"

	wallEnd = stringtointeger( systemfunc("date +%s") )
	wallTime = wallEnd - wallStart
	wallTime@units = "seconds"

	print("=== " + variableName + " Data for " + nTimes + " initializtion times at " + nLeads + " lead times loaded in " + cd_string( wallTime, "%H hr %M min %S sec" ) + " ===" )
	print("=== " + variableName + " CPU time used: " + cd_string( cpuTime, "%H hr %M min %S sec" ) + " ===" )

	printMem("End readGEFS for " + variableName)

	; return
	return variable

	end ;readGEFS

undef("validGEFS")
function validGEFS(										\\
sourcePath[1]       : string,   	\\  ; Root path of the ensemble files. Files should be in sourcePath/yyyy/yyyymm/
gribtype[1]         : string,   	\\  ; Type of file to read (master, pgrba, etc)
members[*]          : string,   	\\  ; List of ensemble members
initTimes[*] 		: double,		\\	; Initialization times in seconds since 01 Jan 1970 00:00 UTC
forecastLeadsHr[*]  : integer,  	\\  ; Forecast lead times in hours
variableName[1]		: string 		\\ 	; Name of variable
)
	local nTimes, nLeads, nMembers, suffix, forecastLeadsHrLarge, membersLarge, validFiles, yyyymmdd, hh, filenames, infiles, validTimes, missingTimes
	begin

	printMem("Begin validGEFS for " + variableName)	
	
	nMembers 	= dimsizes(members)
	nTimes 		= dimsizes(initTimes)
	nLeads 		= dimsizes(forecastLeadsHr)

	suffix = where(gribtype.eq."pgrba", ".grb", ".grb2")

	forecastLeadsHrLarge 	= ndtooned( conform_dims( (/nLeads, nMembers/), forecastLeadsHr, 0 ) )
	membersLarge 			= ndtooned( conform_dims( (/nLeads, nMembers/), members, 1 ) )

	validFiles = new( nTimes, logical )

	do t=0, nTimes-1
		; set up some strings for filename
		yyyymmdd    = cd_string( initTimes(t), "%Y%N%D")
		hh          = cd_string( initTimes(t), "%H")

		; print("Starting valid check for " + yyyymmdd + "_" + hh + "f" + sprinti("%0.2i", forecastLeadsHr))

		if( gribtype.eq."master" ) then
			filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/ge" + membersLarge + ".t" + hh + "z." + gribtype + ".grbf" + sprinti("%0.2i", forecastLeadsHrLarge)
		else if (gribtype.eq."pgrb2ap5") then
			filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/ge" + membersLarge + ".t" + hh + "z.pgrb2a.0p50.f" + sprinti("%0.3i", forecastLeadsHrLarge)
		else if (gribtype.eq."pgrb2a1p0") then
			filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/ge" + membersLarge + ".t" + hh + "z.pgrb2a.1p00.f" + sprinti("%0.3i", forecastLeadsHrLarge)
		else
			filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/ge" + membersLarge + ".t" + hh + "z." + gribtype + "f" + sprinti("%0.2i", forecastLeadsHrLarge)
		end if end if end if

		validFiles(t) = all( isfilepresent( filenames ) )
		
		if(.not.validFiles(t)) then
			print( "Time " + yyyymmdd + "_" + hh + " excluded for variable " + variableName + " because of the following missing files:"  )
			print( "     " + filenames( ind( .not.isfilepresent( filenames ) ) ) )
			print( "" )
			continue
		end if

		infiles = addfiles(filenames + suffix,"r")
		if(STRICT_FILECHECK) then
			do f=0, ListCount(infiles)-1
				if( isdefined ("var_dims") ) then
					validFiles(t) = validFiles(t) .and. isfilevar( infiles[f], variableName ) .and. all( getfilevardims( infiles[f], variableName ) .eq. var_dims )
				else
					validFiles(t) = validFiles(t) .and. isfilevar( infiles[f], variableName )
					var_dims = getfilevardims( infiles[f], variableName )
				end if
				; print( "Strict validity check for " + filenames(f) + ": " + validFiles(t) )
				if(.not.validFiles(t)) then
					print( "Time " + yyyymmdd + "_" + hh + " excluded for variable " + variableName + " because of file " + getfilepath(infiles[f]) )
					break
				end if
			end do
		end if
	end do

	if( isdefined ("var_dims") ) then delete(var_dims) end if

	if( any( validFiles ) ) then
		validTimes = initTimes( ind(validFiles) )
		if( .not.all( validFiles ) ) then
			missingTimes = initTimes( ind(.not.validFiles) )
		end if
	else
		print("No ensemble files found! (readGEFS.validGEFS)")
		print(filenames)
		status_exit(3)
	end if

	; var = readGEFS( sourcePath, gribtype, members, validTimes, forecastLeadsHr, variableName)
	; missing_value = getVarFillValue(validTimes)
	; validTimes@_FillValue = missing_value

	; if( any( abs(var) .gt. 1E6 ) ) then
	; 	dim_time = ind( getvardims(var).eq."initialization_time" )
		; nTimes = dimsizes( validTimes )
		; bad_list = NewList("fifo")
	
		; do t=0, nTimes-1, 1
		; 	var = readGEFS( sourcePath, gribtype, members, validTimes(t), forecastLeadsHr, variableName)
		; 	if( any( abs( var ) .gt. 1E6 ) ) then
		; 		ListPush(bad_list, echo(validTimes(t)))
		; 		validTimes(t) = missing_value
		; 	end if
		; 	delete(var)
		; end do

		; if( any( ismissing(validTimes) ) ) then
		; 	print( num( ismissing(validTimes) ) + " excluded due to suspect data:" )
		; 	print( "     " + cd_string( listToArray(bad_list, typeof(validTimes)), "%Y%N%D_%H") )
		; 	validTimes := validTimes( ind( .not. ismissing(validTimes) ) )
		; end if
	; end if


	; print("Valid times:")
	; print( cd_string( validTimes, "%Y%N%D_%H" ) )

	; print("Missing times:")
	; if( isdefined(missingTimes) ) then
	; 	print( cd_string( initTimes( ind(validFiles) ), "%Y%N%D_%H" ) )
	; else
	; 	print( "none" )
	; end if

	printMem("End validGEFS for " + variableName)

	return validTimes

	end 	; validGEFS


;
; Function to test readGEFS
;
undef("test_readGEFS")
procedure test_readGEFS()
	local sourcePath, initYear, initMonth, initDay, initHour, t2, u
	begin

	sourcePath = "/scratch1/portfolios/NCEPDEV/ensemble/noscrub/Walter.Kolczynski/GEFS/GEFS15_noSTTP"
	initYear = 2014
	initMonth = 1
	initDay = 1
	initHour = 0
	nDays = 7

	leadTimesHr = ispan(0, 120, 6)

	perturb = sprinti( "%0.2i", ispan(0,20,1) )
	members = "p"+perturb
	members(0) = "c00"
	delete(perturb)

	startTime = cd_inv_calendar( initYear, initMonth, initDay, initHour, 0, 0, "seconds since 1970-1-1 00:00:00", 0 )
	endTime = startTime + (nDays - 1) * 86400
	nTimes = doubletointeger( (endTime - startTime) / 86400 ) + 1

	initTimes = fspan( startTime, endTime, nTimes )
	copy_VarAtts( startTime, initTimes )

	print("< ============================================ Single-lead, single-forecast test (old) ============================================>")

	t2 = readGEFS(sourcePath, "pgrba", members, initTimes(0), leadTimesHr(0), "TMP_3_HTGL")
	u = readGEFS(sourcePath, "pgrba", members, initTimes(0), leadTimesHr(0), "U_GRD_3_ISBL")

	;printVarSummary(t2)
	;printVarSummary(u)
	delete(t2)
	delete(u)

	print("< ============================================ Single-lead, multi-forecast test (old) ============================================>")

	t2 = readGEFS(sourcePath, "pgrba", members, initTimes, leadTimesHr(0), "TMP_3_HTGL")
	u = readGEFS(sourcePath, "pgrba", members, initTimes, leadTimesHr(0), "U_GRD_3_ISBL")

	;printVarSummary(t2)
	;printVarSummary(u)
	delete(t2)
	delete(u)

	print("< ============================================ Multi-lead, single-forecast test (old) ============================================>")

	t2 = readGEFS(sourcePath, "pgrba", members, initTimes(0), leadTimesHr, "TMP_3_HTGL")
	u = readGEFS(sourcePath, "pgrba", members, initTimes(0), leadTimesHr, "U_GRD_3_ISBL")

	;printVarSummary(t2)
	;printVarSummary(u)
	delete(t2)
	delete(u)

	print("< ============================================ Multi-lead, multi-forecast test (old) ============================================>")

	t2 = readGEFS(sourcePath, "pgrba", members, initTimes, leadTimesHr, "TMP_3_HTGL")
	u = readGEFS(sourcePath, "pgrba", members, initTimes, leadTimesHr, "U_GRD_3_ISBL")

	;printVarSummary(t2)
	;printVarSummary(u)
	delete(t2)
	delete(u)

	end ;test_readGEFS

undef("readGEFSmean")
function readGEFSmean(									\\
sourcePath[1]       : string,   \\  ; Root path of the ensemble files. Files should be in sourcePath/yyyy/yyyymm/
gribtype[1]         : string,   \\  ; Type of file to read (master, pgrba, etc)
initTimes[*] 		: double,	\\	; Initialization times in seconds since 01 Jan 1970 00:00 UTC
forecastLeadsHr[*]  : integer,  \\  ; Forecast lead times in hours
variableName[1]     : string    \\  ; Name of the desired variable in the grib file
)
	local cpuStart, wallStart, nMembers, nTimes, nLeads, initTimesLarge, forecastLeadsHrLarge, membersLarge, yyyymmdd, hh, filenames, missing, infiles, tempVariable, cpuEnd, cpuTime, \\
			wallEnd, wallTime, variable
	begin

	cpuStart = get_cpu_time()
	wallStart = stringtointeger( systemfunc("date +%s") )

	suffix = where(gribtype.eq."pgrba", ".grb", ".grb2")

	nTimes 		= dimsizes(initTimes)
	nLeads 		= dimsizes(forecastLeadsHr)

	print("=== Reading " + variableName + " data for " + nTimes + " initializtion times at " + nLeads + " lead times ===" )

	initTimesLarge 			= ndtooned( conform_dims( (/nTimes, nLeads/), initTimes, 0 ) )
	copy_VarAtts(initTimes, initTimesLarge)
	forecastLeadsHrLarge 	= ndtooned( conform_dims( (/nTimes, nLeads/), forecastLeadsHr, 1 ) )

	; set up some strings for filename
	yyyymmdd    = cd_string( initTimesLarge, "%Y%N%D")
	hh          = cd_string( initTimesLarge, "%H")

	if( gribtype.eq."master" ) then
		filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/geavg.t" + hh + "z." + gribtype + ".grbf" + sprinti("%0.2i", forecastLeadsHrLarge)
	else if (gribtype.eq."pgrb2ap5") then
		filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/geavg.t" + hh + "z.pgrb2a.0p50.f" + sprinti("%0.3i", forecastLeadsHrLarge)
	else if (gribtype.eq."pgrb2a1p0") then
		filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/geavg.t" + hh + "z.pgrb2a.1p00.f" + sprinti("%0.3i", forecastLeadsHrLarge)
	else
		filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/geavg.t" + hh + "z." + gribtype + "f" + sprinti("%0.2i", forecastLeadsHrLarge)
	end if end if end if
	nFiles = dimsizes(filenames)

	if( any( .not. isfilepresent( filenames ) ) ) then
		missing = ind( .not. isfilepresent( filenames ) )
		print("The following files were not found:")
		print( filenames(missing) )
		print("Exiting...")
		status_exit(2)
	end if

	infiles = addfiles(filenames+suffix, "r")
	ListSetType(infiles, "join")

	tempVariable = infiles[:]->$variableName$

	if(nFiles.eq.1) then
		variable = tempVariable
	else
		tempDimNames = getvardims(tempVariable)
		tempDimSizes = dimsizes(tempVariable)
		nDims =  dimsizes(tempDimSizes) + 1
		dimSizes = new(nDims, integer)

		dimSizes(0) = nTimes
		dimSizes(1) = nLeads
		if(nDims.gt.2) then
			dimSizes(2:) = tempDimSizes(1:)
		end if

		if( product(dimSizes) .ne. product( dimsizes(tempVariable) ) ) then
			print("Something went wrong reading variable data!")
			do i=0, nFiles-1
				print(filenames(i)+"")
				a = infiles[i]->$variableName$
				; printVarSummary(a)
				delete(a)
			end do

			; print(filenames)
			; printVarSummary(tempVariable)
			; print(infiles)
			; print(dimSizes)
			exit
		end if

		variable = reshape( tempVariable, dimSizes )
		copy_VarAtts(tempVariable, variable)

		variable!0 = "initialization_time"
		variable!1 = "lead_time_hr"

		variable&initialization_time 	= initTimes
		variable&lead_time_hr			= forecastLeadsHr

		do d=2, nDims-1
			dimName = tempDimNames(d-1)
			variable!d = dimName
			if( iscoord( tempVariable, dimName ) ) then variable&$dimName$ = tempVariable&$dimName$ end if
		end do
	end if
	delete(tempVariable)

	if( isatt(variable, "forecast_time") ) then delete( variable@forecast_time ) end if
	if( isatt(variable, "forecast_time_units") ) then delete( variable@forecast_time_units ) end if
	if( isatt(variable, "initial_time") ) then delete( variable@initial_time ) end if

	cpuEnd = get_cpu_time()
	cpuTime = cpuEnd - cpuStart
	cpuTime@units = "seconds"

	wallEnd = stringtointeger( systemfunc("date +%s") )
	wallTime = wallEnd - wallStart
	wallTime@units = "seconds"

	print("=== " + variableName + " Data for " + nTimes + " initializtion times at " + nLeads + " lead times loaded in " + cd_string( wallTime, "%H hr %M min %S sec" ) + " ===" )
	print("=== " + variableName + " CPU time used: " + cd_string( cpuTime, "%H hr %M min %S sec" ) + " ===" )

	; remove degenerate dimensions and return
	return rm_single_dims(variable)
	; return variable

	end 	; readGEFSmean

undef("readGEFSspread")
function readGEFSspread(									\\
sourcePath[1]       : string,   \\  ; Root path of the ensemble files. Files should be in sourcePath/yyyy/yyyymm/
gribtype[1]         : string,   \\  ; Type of file to read (master, pgrba, etc)
initTimes[*] 		: double,	\\	; Initialization times in seconds since 01 Jan 1970 00:00 UTC
forecastLeadsHr[*]  : integer,  \\  ; Forecast lead times in hours
variableName[1]     : string    \\  ; Name of the desired variable in the grib file
)
	local cpuStart, wallStart, nMembers, nTimes, nLeads, initTimesLarge, forecastLeadsHrLarge, membersLarge, yyyymmdd, hh, filenames, missing, infiles, tempVariable, cpuEnd, cpuTime, \\
			wallEnd, wallTime, variable
	begin

	cpuStart = get_cpu_time()
	wallStart = stringtointeger( systemfunc("date +%s") )

	suffix = where(gribtype.eq."pgrba", ".grb", ".grb2")

	nTimes 		= dimsizes(initTimes)
	nLeads 		= dimsizes(forecastLeadsHr)

	initTimesLarge 			= ndtooned( conform_dims( (/nTimes, nLeads/), initTimes, 0 ) )
	copy_VarAtts(initTimes, initTimesLarge)
	forecastLeadsHrLarge 	= ndtooned( conform_dims( (/nTimes, nLeads/), forecastLeadsHr, 1 ) )

	; set up some strings for filename
	yyyymmdd    = cd_string( initTimesLarge, "%Y%N%D")
	hh          = cd_string( initTimesLarge, "%H")

	if( gribtype.eq."master" ) then
		filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/gespr.t" + hh + "z." + gribtype + ".grbf" + sprinti("%0.2i", forecastLeadsHrLarge)
	else if (gribtype.eq."pgrb2ap5") then
		filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/gespr.t" + hh + "z.pgrb2a.0p50.f" + sprinti("%0.3i", forecastLeadsHrLarge)
	else if (gribtype.eq."pgrb2a1p0") then
		filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/gespr.t" + hh + "z.pgrb2a.1p00.f" + sprinti("%0.3i", forecastLeadsHrLarge)
	else
		filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/gespr.t" + hh + "z." + gribtype + "f" + sprinti("%0.2i", forecastLeadsHrLarge)
	end if end if end if
	nFiles = dimsizes(filenames)

	if( any( .not. isfilepresent( filenames ) ) ) then
		missing = ind( .not. isfilepresent( filenames ) )
		print("The following files were not found:")
		print( filenames(missing) )
		print("Exiting...")
		status_exit(2)
	end if

	infiles = addfiles(filenames+suffix, "r")
	ListSetType(infiles, "join")

	tempVariable = infiles[:]->$variableName$

	if(nFiles.eq.1) then
		variable = tempVariable
	else
		tempDimNames = getvardims(tempVariable)
		tempDimSizes = dimsizes(tempVariable)
		nDims =  dimsizes(tempDimSizes) + 1
		dimSizes = new(nDims, integer)

		dimSizes(0) = nTimes
		dimSizes(1) = nLeads
		if(nDims.gt.2) then
			dimSizes(2:) = tempDimSizes(1:)
		end if

		variable = reshape( tempVariable, dimSizes )
		copy_VarAtts(tempVariable, variable)

		variable!0 = "initialization_time"
		variable!1 = "lead_time_hr"

		variable&initialization_time 	= initTimes
		variable&lead_time_hr			= forecastLeadsHr

		do d=2, nDims-1
			dimName = tempDimNames(d-1)
			variable!d = dimName
			if( iscoord( tempVariable, dimName ) ) then variable&$dimName$ = tempVariable&$dimName$ end if
		end do
	end if
	delete(tempVariable)

	if( isatt(variable, "forecast_time") ) then delete( variable@forecast_time ) end if
	if( isatt(variable, "forecast_time_units") ) then delete( variable@forecast_time_units ) end if
	if( isatt(variable, "initial_time") ) then delete( variable@initial_time ) end if

	cpuEnd = get_cpu_time()
	cpuTime = cpuEnd - cpuStart
	cpuTime@units = "seconds"

	wallEnd = stringtointeger( systemfunc("date +%s") )
	wallTime = wallEnd - wallStart
	wallTime@units = "seconds"

	print("=== " + variableName + " Data for " + nTimes + " initializtion times at " + nLeads + " lead times loaded in " + cd_string( wallTime, "%H hr %M min %S sec" ) + " ===" )
	print("=== " + variableName + " CPU time used: " + cd_string( cpuTime, "%H hr %M min %S sec" ) + " ===" )

	; remove degenerate dimensions and return
	return rm_single_dims(variable)
	; return variable

	end 	; readGEFSspread

;
; Function to test readGEFS
;
undef("test_readGEFSmean")
procedure test_readGEFSmean()
	local sourcePath, initYear, initMonth, initDay, initHour, t2, u
	begin

	sourcePath = "/scratch1/portfolios/NCEPDEV/ensemble/noscrub/Walter.Kolczynski/GEFS/tfFu"
	initYear = 2013
	initMonth = 9
	initDay = 12
	initHour = 0
	nDays = 1

	; leadTimesHr = ispan(0, 120, 6)
	leadTimesHr = 192

	startTime = cd_inv_calendar( initYear, initMonth, initDay, initHour, 0, 0, "seconds since 1970-1-1 00:00:00", 0 )
	endTime = startTime + (nDays - 1) * 86400
	nTimes = doubletointeger( (endTime - startTime) / 86400 ) + 1

	initTimes = fspan( startTime, endTime, nTimes )
	copy_VarAtts( startTime, initTimes )

	do t=0, nTimes-1
		print( "" + cd_string( initTimes(t), "%Y%N%D") )
		a = readGEFSmean(sourcePath, "pgrba", initTimes(0:t), 288, "U_GRD_3_HTGL")
		s = dimsizes(a)
		if(t.gt.1 .and. s(0).ne.t+1) then 
			exit
		end if
		delete(a)
		delete(s)
	end do

	; t2mean 	= readGEFSmean(sourcePath, "pgrba", initTimes, leadTimesHr(4), "TMP_3_HTGL")
	; t2sd 	= readGEFSspread(sourcePath, "pgrba", initTimes, leadTimesHr(4), "TMP_3_HTGL")

	; printVarSummary(t2mean)
	; printVarSummary(t2sd)

	end ;test_readGEFS

undef("validGEFSmean")
function validGEFSmean(										\\
sourcePath[1]       : string,   	\\  ; Root path of the ensemble files. Files should be in sourcePath/yyyy/yyyymm/
gribtype[1]         : string,   	\\  ; Type of file to read (master, pgrba, etc)
initTimes[*] 		: double,		\\	; Initialization times in seconds since 01 Jan 1970 00:00 UTC
forecastLeadsHr[*]  : integer,  	\\  ; Forecast lead times in hours
variableName[1]		: string 		\\ 	; Name of variable
)
	local nTimes, nLeads, yyyymmdd, hh, filenames, validFiles, validTimes
	begin

	nTimes 		= dimsizes(initTimes)
	nLeads 		= dimsizes(forecastLeadsHr)

	suffix = where(gribtype.eq."pgrba", ".grb", ".grb2")

	validFiles = new( nTimes, logical )

	do t=0, nTimes-1
		; set up some strings for filename
		; print( cd_string( initTimes(t), "Checking %Y%N%D_%H" ) + "" )

		yyyymmdd    = cd_string( initTimes(t), "%Y%N%D")
		hh          = cd_string( initTimes(t), "%H")
		if( gribtype.eq."master" ) then
			filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/geavg.t" + hh + "z." + gribtype + ".grbf" + sprinti("%0.2i", forecastLeadsHr)
		else if (gribtype.eq."pgrb2ap5") then
			filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/geavg.t" + hh + "z.pgrb2a.0p50.f" + sprinti("%0.3i", forecastLeadsHr)
		else if (gribtype.eq."pgrb2a1p0") then
			filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/geavg.t" + hh + "z.pgrb2a.1p00.f" + sprinti("%0.3i", forecastLeadsHr)
		else
			filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/geavg.t" + hh + "z." + gribtype + "f" + sprinti("%0.2i", forecastLeadsHr)
		end if end if end if

		validFiles(t) = all( isfilepresent( filenames ) )

		; print( "avg" + validFiles(t) )
		if(.not.validFiles(t)) then continue end if

		if( validFiles(t) .and. STRICT_FILECHECK) then
			infiles = addfiles(filenames + suffix,"r")

			nValid = num( validFiles(:t) )

			do f=0, ListCount(infiles)-1
				validFiles(t) = validFiles(t) .and. isfilevar( infiles[f], variableName )

				; Make sure all dim sizes are the same
				dim_sizes_t =  getfilevardimsizes( infiles[f], variableName )
				if( .not. isdefined("dim_sizes") ) then
					dim_sizes = dim_sizes_t
				end if

				if( any( dim_sizes .ne. dim_sizes_t ) ) then
					if( nValid .eq. 2 ) then
						dim_sizes_2 = dim_sizes_t
					else
						if( isdefined("dim_sizes_2") .and. all( dim_sizes_2 .eq. dim_sizes_t ) ) then
							dim_sizes = dim_sizes_2
							valid = ind(validFiles)
							validFiles( valid(0) ) = False
							delete(valid)
							delete(dim_sizes_2)
						else
							validFiles(t) = False
						end if
					end if
				else
					if( isdefined("dim_sizes_2") ) then
						valid = ind(validFiles)
						validFiles( valid(1) ) = False
						delete(valid)
						delete(dim_sizes_2)
					end if
				end if
			end do

			if(.not.validFiles(t)) then continue end if

		end if

		if(.not.validFiles(t)) then continue end if

		if( gribtype.eq."master" ) then
			filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/gespr.t" + hh + "z." + gribtype + ".grbf" + sprinti("%0.2i", forecastLeadsHr)
		else if (gribtype.eq."pgrb2ap5") then
			filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/gespr.t" + hh + "z.pgrb2a.0p50.f" + sprinti("%0.3i", forecastLeadsHr)
		else if (gribtype.eq."pgrb2a1p0")
			filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/gespr.t" + hh + "z.pgrb2a.1p00.f" + sprinti("%0.3i", forecastLeadsHr)		
		else
			filenames = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/" + gribtype + "/gespr.t" + hh + "z." + gribtype + "f" + sprinti("%0.2i", forecastLeadsHr)
		end if end if end if
		validFiles(t) = all( isfilepresent( filenames ) ) .and. validFiles(t)
		; print("spr" + validFiles(t))
		
		if(validFiles(t).and.STRICT_FILECHECK) then
			infiles = addfiles(filenames + suffix,"r")
			do f=0, ListCount(infiles)-1
				validFiles(t) = validFiles(t) .and. isfilevar( infiles[f], variableName )
			end do
		end if
	end do

	if( any( validFiles ) ) then
		validTimes = initTimes( ind(validFiles) )
		if( .not.all( validFiles ) ) then
			missingTimes = initTimes( ind(.not.validFiles) )
		end if
	else
		print("No ensemble files found! (readGEFS.validGEFSmean)")
		print(validFiles + " " + filenames)
		print(infiles)
		status_exit(3)
	end if
	
	; var = readGEFSmean( sourcePath, gribtype, initTimes( ind(validFiles) ), forecastLeadsHr, variableName)
	; missing_value = getVarFillValue(validTimes)
	; validTimes@_FillValue = missing_value

	; if( any( abs(var) .gt. 1E6 ) ) then
	; 	dim_time = ind( getvardims(var).eq."initialization_time" )
	; 	nTimes = dimsizes( validTimes )
	; 	missing_list = NewList("fifo")
	
	; 	do t=0, nTimes-1, 1
	; 		; var = readGEFSmean( sourcePath, gribtype, validTimes(t), forecastLeadsHr, variableName)
	; 		; if( any( abs( var ) .gt. 1E6 ) ) then
	; 		if( any( abs( slice( var, dim_time, t ) ) .gt. 1E6 ) ) then
	; 			ListPush(missing_list, echo(validTimes(t)))
	; 			validTimes(t) = missing_value
	; 		end if
	; 		; delete(var)
	; 	end do

	; 	if( any( ismissing(validTimes) ) ) then
	; 		print( num( ismissing(validTimes) ) + " excluded due to suspect data:" )
	; 		print( "     " + cd_string( listToArray(missing_list, typeof(validTimes)), "%Y%N%D_%H") )
	; 		validTimes := validTimes( ind( .not. ismissing(validTimes) ) )
	; 	end if
	; end if

	; delete(var)

	; print("Valid times:")
	; print( cd_string( validTimes, "%Y%N%D_%H" ) )

	; print("Missing times:")
	; if( isdefined(missingTimes) ) then
	; 	print( cd_string( initTimes( ind(validFiles) ), "%Y%N%D_%H" ) )
	; else
	; 	print( "none" )
	; end if

	return validTimes

	end 	; validGEFSmean

undef("readGEFSpost")
function readGEFSpost(                                  \\
sourcePath[1]       : string,   \\  ; Root path of the ensemble files. Files should be in sourcePath/yyyy/yyyymm/
initYear[1]         : integer,  \\  ; Initialization year
initMonth[1]        : integer,  \\  ; Initialization month
initDay[1]          : integer,  \\  ; Initialization day
initHour[1]         : integer,  \\  ; Initialization hour
variableName[1]     : string    \\  ; Name of the desired variable in the grib file
)
	local yyyyymmdd, hh, filename, infile, variable
	begin

	; suffix = where(gribtype.eq."pgrba", ".grb", ".grb2")

	; set up some strings for filename
	yyyymmdd    = initYear + sprinti("%0.2i",initMonth) + sprinti("%0.2i",initDay)
	hh          = sprinti("%0.2i",initHour)
	filename = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/ensstat/enspost.t" + hh + "z." + variableName

	infile = addfile(filename + ".grb", "r");
	variable = infile->$VARIABLE_NAME_HASH( {variableName} )$
	return variable

	end ;readGEFSpost

undef("readGEFSpostTest")
procedure readGEFSpostTest()
	begin

	sourcePath = "/scratch1/portfolios/NCEPDEV/ensemble/noscrub/Walter.Kolczynski/GEFS15_noSTTP"
	initYear = 2013
	initMonth = 5
	initDay = 16
	initHour = 0

	variableName = "t2m"
	t2 = readGEFSpost(sourcePath, initYear, initMonth, initDay, initHour, variableName)

	variableName = "u850"
	u850 = readGEFSpost(sourcePath, initYear, initMonth, initDay, initHour, variableName)

	printVarSummary(t2)
	printVarSummary(u850)

	end ;readGEFSpostTest

	undef("readGEFSstat")
	function readGEFSstat(                                  \\
							sourcePath[1]       : string,   \\  ; Root path of the ensemble files. Files should be in sourcePath/yyyy/yyyymm/
							initYear[1]         : integer,  \\  ; Initialization year
							initMonth[1]        : integer,  \\  ; Initialization month
							initDay[1]          : integer,  \\  ; Initialization day
							initHour[1]         : integer,  \\  ; Initialization hour
							variableName[1]     : string    \\  ; Name of the desired variable in the grib file
							)
	local yyyyymmdd, hh, filename, infile, variable
	begin

	; set up some strings for filename
	yyyymmdd    = initYear + sprinti("%0.2i",initMonth) + sprinti("%0.2i",initDay)
	hh          = sprinti("%0.2i",initHour)
	filename = sourcePath + "/gefs." + yyyymmdd + "/" + hh + "/ensstat/ensstat.t" + hh + "z." + variableName


	setfileoption("grb", "SingleElementDimensions", "All")
	infile = addfile(filename + ".grb", "r");
	list_filevars(infile)
	variable = infile->$VARIABLE_NAME_HASH( {variableName} )$
	return variable

	end ;readGEFSstat

undef("readGEFSstatTest")
procedure readGEFSstatTest()
	begin

	sourcePath = "/scratch1/portfolios/NCEPDEV/ensemble/noscrub/Walter.Kolczynski/GEFS15_noSTTP"
	initYear = 2013
	initMonth = 5
	initDay = 16
	initHour = 0

	variableName = "t2m"
	t2 = readGEFSstat(sourcePath, initYear, initMonth, initDay, initHour, variableName)

	variableName = "u850"
	u850 = readGEFSstat(sourcePath, initYear, initMonth, initDay, initHour, variableName)

	printVarSummary(t2)
	printVarSummary(u850)

	; print(t2(:,:,5,5))

	end ;readGEFSstatTest
