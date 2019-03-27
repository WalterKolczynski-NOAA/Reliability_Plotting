; readGDAS.ncl
;
;
; read GDAS analysis file
;
; load other scripts
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/contrib/cd_string.ncl"

load "utilityFuncs.ncl"
load "addNewDim.ncl"

;
; Read the GDAS data at one initialization time
;   Source GDAS files should be named in the following pattern:
;       <sourcePath>/gridType/YYYY/YYYYMM/gdas_gridType_YYYYMMDD_HH;
;
undef("readGDAS_single")
function readGDAS_single(                       \\
			sourcePath[1]       : string,   \\  ; Root path of the ensemble files. Files should be in sourcePath/yyyy/yyyymm/
			gridType[1]			: string,	\\	; Type of file to read (pgrb, pgrbh)
			validTime[1] 		: double,	\\	; Initialization time in seconds since 01 Jan 1970 00:00 UTC
			variableName[1]     : string    \\  ; Name of the desired variable in the WRF file
			)
local perturb, members, yyyymmdd, hh, filename, infile, variable, cpuStart, cpuEnd, cpuTime
begin

cpuStart = get_cpu_time()
print("Reading verification data for " + variableName + " at " + cd_string( validTime, "%Y%N%D%H") )

; turn off warnings to prevent unidentified grid messages
err = NhlGetErrorObjectId()
setvalues err
	"errLevel" : "Fatal"          ; only report Fatal errors
end setvalues

; set up some strings for filename
yyyy 		= cd_string( validTimes, "%Y")
yyyymm 		= cd_string( validTimes, "%Y%N")
yyyymmdd    = cd_string( validTime, "%Y%N%D")
hh          = cd_string( validTime, "%H")

filenames = sourcePath + "/" + gridType + "/" + yyyy + "/" + yyyymm + "/gdas_" + gridType + "_" + yyyymmdd + "_" + hh;

; insert check files exist

infile = addfiles(filename + ".grb", "r")
variable = infile->$variableName$

; turn warnings back on
setvalues err
	"errLevel" : "Warning"
end setvalues

cpuEnd = get_cpu_time()

cpuTime = cpuEnd - cpuStart
cpuTime@units = "seconds"

print("   Data loaded in " + cd_string( cpuTime, "%H hr %M min %S sec" ) )

return variable

end ;readGDAS_single

;
; Function to test readGDAS_single
;
undef("test_readGDAS_single")
procedure test_readGDAS_single()
local sourcePath, initYear, initMonth, initDay, initHour, t2, u
begin

sourcePath = "/scratch1/portfolios/NCEPDEV/ensemble/noscrub/Walter.Kolczynski/GFSanl/1.0_deg"
initYear = 2014
initMonth = 6
initDay = 1
initHour = 0

validTime = cd_inv_calendar( initYear, initMonth, initDay, initHour, 0, 0, "seconds since 1970-1-1 00:00:00", 0 )

t2	= readGDAS_single(sourcePath, "pgrbh", validTime, "TMP_GDS4_HTGL")
u	= readGDAS_single(sourcePath, "pgrb", validTime, "U_GRD_GDS4_ISBL")

printVarSummary(t2)
printVarSummary(u)

end ;test_readGDAS_single

;
; Read GDAS files at multiple verification times
;
undef("readGDAS")
function readGDAS(									\\
						sourcePath[1]       : string,   \\  ; Root path of the ensemble files. Files should be in sourcePath/yyyy/yyyymm/
						gridType[1]			: string,	\\	; Type of file to read (pgrb, pgrbh)
						validTimes[*] 		: double,	\\	; Initialization times in seconds since 01 Jan 1970 00:00 UTC
						variableName[1]     : string    \\  ; Name of the desired variable in the WRF file
					  )
local cpuStart, nTimes, yyyy, yyyymm, yyyymmdd, hh, filenames, err, infiles, variable, cpuEnd, cpuTime
begin

cpuStart = get_cpu_time()

nTimes = dimsizes(validTimes)
print("Reading verification data for " + variableName + " from " + cd_string( validTimes(0), "%Y%N%D%H") + " to " + cd_string( validTimes(nTimes-1), "%Y%N%D%H") )

; set up some strings for filename
; yyyy 		= cd_string( validTimes, "%Y")
; yyyymm 		= cd_string( validTimes, "%Y%N")
; yyyymmdd 	= cd_string( validTimes, "%Y%N%D")
; hh 			= cd_string( validTimes, "%H")

; filenames = sourcePath + "/" + gridType + "/" + yyyy + "/" + yyyymm + "/gdas_" + gridType + "_" + yyyymmdd + "_" + hh;
filenames = cd_string(validTimes, sourcePath + "/" + gridType + "/%Y/%Y%N/gdas_" + gridType + "_%Y%N%D_%H")

; turn off warnings to prevent unidentified grid messages
err = NhlGetErrorObjectId()
setvalues err
	"errLevel" : "Fatal"          ; only report Fatal errors
end setvalues

infiles = addfiles(filenames+".grb", "r")
ListSetType(infiles, "join")
variable = infiles[:]->$variableName$
if( dimsizes(validTimes).gt.1 ) then
	variable!0 = "valid_time"
	variable&valid_time = validTimes
else
	variable_temp = variable
	variable := addNewDim( variable_temp, validTimes, "valid_time", 0 )
	fillNewDim( variable, variable_temp, 0, 0 )
	delete(variable_temp)
end if

; turn warnings back on
setvalues err
	"errLevel" : "Warning"
end setvalues

cpuEnd = get_cpu_time()
cpuTime = cpuEnd - cpuStart
cpuTime@units = "seconds"

print("=== " + variableName + " verification data for " + nTimes + " valid times loaded in " + cd_string( cpuTime, "%H hr %M min %S sec" ) + " ===" )

return variable

end ;readGDAS

;
; Function to test readGDAS
;
undef("test_readGDAS")
procedure test_readGDAS()
local sourcePath, initYear, initMonth, initDay, initHour, t2, u
begin

sourcePath = "/scratch1/portfolios/NCEPDEV/ensemble/noscrub/Walter.Kolczynski/GDAS"
initYear = 2013
initMonth = 5
initDay = 18
initHour = 0
nDays = 3

startTime = cd_inv_calendar( initYear, initMonth, initDay, initHour, 0, 0, "seconds since 1970-1-1 00:00:00", 0 )
endTime = startTime + (nDays - 1) * 86400
nTimes = doubletointeger( (endTime - startTime) / 86400 ) + 1

validTimes = fspan( startTime, endTime, nTimes )
copy_VarAtts( startTime, validTimes )

t2	= readGDAS(sourcePath, "1.0deg", validTimes, "TMP_GDS4_HTGL")
u 	= readGDAS(sourcePath, "1.0deg", validTimes, "U_GRD_GDS4_ISBL")

printVarSummary(t2)
printVarSummary(u)

end ;testReadMassGDAS

undef("validGDAS")
function validGDAS(									\\
						sourcePath[1]       : string,   \\  ; Root path of the ensemble files. Files should be in sourcePath/yyyy/yyyymm/
						gridType[1]			: string,	\\	; Type of file to read (pgrb, pgrbh)
						validTimes[*] 		: double    \\  ; Name of the desired variable in the WRF file
					  )
local yyyy, yyyymm, yyyymmdd, hh, filenames
begin

; set up some strings for filename
yyyy 		= cd_string( validTimes, "%Y")
yyyymm 		= cd_string( validTimes, "%Y%N")
yyyymmdd 	= cd_string( validTimes, "%Y%N%D")
hh 			= cd_string( validTimes, "%H")

filenames = sourcePath + "/" + gridType + "/" + yyyy + "/" + yyyymm + "/gdas_" + gridType + "_" + yyyymmdd + "_" + hh;

fileExists = fileexists( filenames )

if( any( filesExist ) ) then
	validTimes = validTimes( ind(filesExist) )
	if( .not.all( filesExist ) ) then
		missingTimes = validTimes( ind(.not.filesExist) )
	end if
else
	print("No verification files found!")
	status_exit(3)
end if

print("Valid times:")
print( cd_string( validTimes, "%Y%N%D_%H" ) )
print("")

print("Missing times:")
if( isdefined(missingTimes) ) then
	print( cd_string( validTimes( ind(filesExist) ), "%Y%N%D_%H" ) )
else
	print( "none" )
end if

return validTimes

end 	; validGDAS
