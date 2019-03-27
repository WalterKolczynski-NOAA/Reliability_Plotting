load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/contrib/cd_string.ncl"

load "readGEFS.ncl"
load "readGDAS.ncl"
load "readConsensus.ncl"
load "readClimo.ncl"
load "utilityFuncs.ncl"

FILETYPE_MAP = "png"
; FILETYPE_MAP = "X11"
; FILETYPE_MAP@wkHeight = 800
; FILETYPE_MAP@wkWidth  = 800
FILETYPE_SS = "png"
TRIM_WIDTH = 112
PLOT_RESIZE = "75%"

SYNC_TIMES = True	; Only

; Temporary
perturb = sprinti( "%0.2i", ispan(0,20,1) )
members = "p"+perturb
members(0) = "c00"
delete(perturb)

; For spread-skill plots
N_ENS = 21 			; Only used for Kolczynski et al. 2011 corrections (currently commented out)
BIN_SIZE = 10000	; Number of points per bin

; Functions
	; Sort
	if isdefined("sortIntoBins") then undef("sortIntoBins") end if
	function sortIntoBins 	\\
	( 						\\
	binSize		: integer,	\\
	x[*]		: numeric,	\\
	y[*]		: numeric	\\
	)
		local nX, nBins, sortLoc, bins, xBinAvg, yBinAvg, xBinStdDev, yBinStdDev
		begin

		; determine number of bins
		nX = dimsizes(x)
		nBins = nX/binSize

		; sort by x
		sortLoc = dim_pqsort( x, 1 )
		x = x(sortLoc)
		y = y(sortLoc)

		bins 		= new( (/nBins+1/), typeof(x) )
		xBinAvg 	= new( (/nBins/), typeof(x) )
		xBinStdDev 	= new( (/nBins/), typeof(x) )
		yBinAvg 	= new( (/nBins/), typeof(y) )
		yBinStdDev 	= new( (/nBins/), typeof(y) )

		do i=0, nBins-1
			binBegin 	= i*binSize
			binEnd 		= binBegin+binSize-1
			bins(i)		= x( binBegin )
			bins(i+1) 	= x( binEnd )
			xBinAvg(i) 		= avg( x(binBegin:binEnd) )
			yBinAvg(i) 		= avg( y(binBegin:binEnd) )
			xBinStdDev(i) 	= stddev( x(binBegin:binEnd) )
			yBinStdDev(i) 	= stddev( y(binBegin:binEnd) )
		end do

		bins@xBinAvg = xBinAvg
		bins@yBinAvg = yBinAvg
		bins@xBinStdDev = xBinStdDev
		bins@yBinStdDev = yBinStdDev

		return bins
		end 	; sortIntoBins

	if isdefined("findLevelDim") then undef("findLevelDim") end if
	function findLevelDim	\\
	(						\\
	variable 	: numeric	\\
	)
		local dimNames, dimName, i
		begin

		dimNames = getvardims(variable)
		do i=0, dimsizes(dimNames)-1
			dimName = dimNames(i)
			if( str_get_cols(dimName, 0, 2) .eq. "lv_" ) then
				dimName@index = i
				return dimName
			end if
		end do

		; print("No dimension name begining with 'lv_' found!")
		; printVarSummary(variable)

		dimName = default_fillvalue("string")
		dimName@index = -1
		return dimName

		end ; findLevelDim

	if isdefined("rectifyLevels") then undef("rectifyLevels") end if
	function rectifyLevels	\\
	(
	forecast 	: numeric, 	\\
	obs 		: numeric 	\\
	)
		local forecastLevelDim, obsLevelDim, obsLevels, forecastLevels, newObs, index
		begin

		newObs = obs

		forecastLevelDim = findLevelDim(forecast)
		obsLevelDim = findLevelDim(obs)

		if( ismissing(forecastLevelDim) .and. .not. ismissing(obsLevelDim) ) then
			print("Slicing single forecast level " + forecast@level + " from multi-level obs")
			obsLevels = obs&$obsLevelDim$
			index = ind(obsLevels.eq.forecast@level)
			if(.not. ismissing(index) ) then
				newObs := slice( newObs, obsLevelDim@index, index )
			else
				print("FATAL: level not found in obs!")
			end if
		else
			if ( .not. ismissing(forecastLevelDim) .and. .not. ismissing(obsLevelDim) ) then
				;
				; GDAS has more vertical levels than the ensemble forecast, so regrid in vertical if the field has a vertical component
				;
				forecastLevels = forecast&$forecastLevelDim$
				obsLevels = newObs&$obsLevelDim$

				if(obsLevels@units.ne.forecastLevels@units) then
					if(obsLevels@units.eq."hPa" .and. forecastLevels@units.eq."Pa") then
						obsLevels = obsLevels * 100
						obsLevels@units = "Pa"
					end if
					if(obsLevels@units.eq."Pa" .and. forecastLevels@units.eq."hPa") then
						obsLevels = obsLevels / 100
						obsLevels@units = "hPa"
					end if
				end if

				if( dimsizes(forecastLevels).ne.dimsizes(obsLevels) .or. .not. all( forecastLevels .eq. obsLevels ) ) then
					print("Reducing " + obsLevelDim + " in " + obs@long_name + " to " + forecastLevelDim)
					newObs := int2p_n_Wrap( obsLevels, newObs, forecastLevels, 1, obsLevelDim@index )
				end if
			end if
		end if

		return newObs

		end ; rectifyLevels

	if( isdefined("loadDefaultMapRes") ) then undef("loadDefaultMapRes") end if
	function loadDefaultMapRes()
		local mapRes
		begin
		mapRes = True

		; do not advance frame immediately
		mapRes@gsnDraw					= False
		mapRes@gsnFrame					= False
		mapRes@gsnAddCyclic 			= True

		; map attributes
		mapRes@mpDataBaseVersion     	= "MediumRes"     ; Default is LowRes
		mapRes@mpOutlineDrawOrder    	= "PostDraw"      ; Draw map outlines last
		mapRes@mpDataSetName			= "Earth..4"
		mapRes@mpGridLineDashPattern	= 2			; Dashed lat/lon Lines
		mapRes@mpGridAndLimbOn			= True		; Draw lat/lon Lines
		mapRes@mpFillOn					= False		; don't fill land grey
		mapRes@mpProjection    			= "Hammer"
		mapRes@gsnStringFontHeightF 	= 0.017
		mapRes@mpPerimOn 				= False

		; tick mark attributes
		mapRes@tmXTOn					= False		; Turn off top tick marks
		mapRes@tmXTLabelsOn				= False		; Turn off top tick labels
		mapRes@tmYROn					= False		; Turn off right tick marks
		mapRes@tmYRLabelsOn				= False		; Turn off right tick labels

		; contour attributes
		mapRes@cnFillOn					= True
		mapRes@cnFillMode				= "RasterFill"
		mapRes@cnLinesOn				= False
		mapRes@cnLineLabelsOn			= False
		mapRes@cnInfoLabelOn    		= False
		mapRes@cnLevelSelectionMode		= "ManualLevels"
		mapRes@cnSpanFillPalette		= True

		; label bar
		mapRes@lbLabelBarOn 			= False					; we are plotting a joint label bar, so turn of the individual ones

		return mapRes
		end 	; loadDefaultMapRes

	if( isdefined("loadDefaultPanelRes") ) then undef("loadDefaultPanelRes") end if
	function loadDefaultPanelRes()
		local panelRes
		begin
		panelRes						= True
		panelRes@gsnPanelLabelBar		= True
		panelRes@lbLabelAutoStride		= True
		panelRes@lbOrientation 			= "Vertical"
		panelRes@lbLabelFontHeightF		= 0.012
		panelRes@gsnFrame				= False
		panelRes@gsnDraw				= False

		return panelRes
		end 	; loadDefaultPanelRes

	if( isdefined("loadDefaultZonalRes") ) then undef("loadDefaultZonalRes") end if 
	function loadDefaultZonalRes()
		local zonalRes
		begin
		zonalRes 						= True
		zonalRes@gsnDraw				= False
		zonalRes@gsnFrame				= False

		zonalRes@gsnStringFontHeightF 	= 0.038

		; tick mark attributes
		zonalRes@tmXTOn					= False		; Turn off top tick marks
		zonalRes@tmXTLabelsOn			= False		; Turn off top tick labels
		zonalRes@tmYROn					= False		; Turn off right tick marks
		zonalRes@tmYRLabelsOn			= False		; Turn off right tick labels


		; contour attributes
		zonalRes@cnFillOn				= True
		zonalRes@cnFillMode				= "RasterFill"
		zonalRes@cnLinesOn				= False
		zonalRes@cnLineLabelsOn			= False
		zonalRes@cnInfoLabelOn    		= False
		zonalRes@cnLevelSelectionMode	= "ManualLevels"
		zonalRes@cnSpanFillPalette		= True

		; axis attributes
		zonalRes@trXReverse 			= True
		zonalRes@tmYRMode             	= "Automatic"
		zonalRes@trYMinF				= 100
		zonalRes@vpWidthF				= 1.5

		; label bar
		zonalRes@lbLabelBarOn 			= False					; we are plotting a joint label bar, so turn of the individual ones

		return zonalRes
		end 	; loadDefaultZonalRes

	if( isdefined("loadDefaultSpreadSkillRes") ) then undef("loadDefaultSpreadSkillRes") end if 
	function loadDefaultSpreadSkillRes()
		local xyRes
		begin

		xyRes = True

		xyRes@tmXTOn = False
		xyRes@tmYROn = False

		xyRes@tiXAxisString = "Bin Mean Ensemble Variance"
		xyRes@tiYAxisString = "Intra-bin Error Variance"

		xyRes@tiMainConstantSpacingF 	= 1.0

		return xyRes

		end 	; loadDefaultSpreadSkillRes

	if( isdefined("loadDefaultNormSpreadSkillRes") ) then undef("loadDefaultNormSpreadSkillRes") end if 
	function loadDefaultNormSpreadSkillRes()
		local xyRes
		begin

		xyRes = True

		xyRes@tmXTOn = False
		xyRes@tmYROn = False

		xyRes@tiXAxisString = "Bin Mean Variance of Ensemble/Climo"
		xyRes@tiYAxisString = "Intra-bin Variance of Error/Climo"

		xyRes@tiMainConstantSpacingF 	= 1.0

		return xyRes

		end 	; loadDefaultSpreadSkillRes

	if( isdefined("readPlotAtts") ) then undef("readPlotAtts") end if
	function readPlotAtts( res[1]: logical, filename[1]: string )
		local input, atts, values, nAtts, a
		begin

		print( "Reading attributes from file " + filename )

		nclVer = str_get_cols( get_ncl_version(), 0, 3 )
		if( .not. fileexists(filename) ) then
			print( "Attribute file " + filename + " not found, using defaults" )
			return res
		end if
		input = str_split_csv( asciiread( filename, -1, "string" ), "=", 1 )
		; print(input)
		if( input(0,0).eq."missing" ) then return res end if
		atts = input(:,0)
		values = input(:,1)
		nAtts = dimsizes(atts)

		res = True

		do a=0, nAtts-1
			if( isatt( res, atts(a) ) ) then delete( res@$atts(a)$ ) end if
			; print( "Setting " + atts(a) + " to " + values(a) + " from " + filename )
			res@$atts(a)$ = values(a)
		end do 	; a

		return res

		end 	; readPlotAtts

	if( isdefined("makeMapSpreadPlot") ) then undef("makeMapSpreadPlot") end if
	procedure makeMapSpreadPlot (				\\
	outfile[1] 				: graphic,	\\
	ensSDtimeAvg[*][*]		: numeric,	\\
	rmseTimeAvg[*][*] 		: numeric,	\\
	spreadSkillRatio[*][*] 	: numeric, 	\\
	spreadMapRes[1]			: logical,	\\
	rmseMapRes[1]			: logical,	\\
	ratioMapRes[1]			: logical,	\\
	twoPlotPanelRes[1]		: logical,	\\
	bottomPanelRes[1]		: logical	\\
	)
		local plots, errorPanelId, mapPanelId, outlierPanelId
		begin

		plots = new(3, graphic)
		plots(0) = gsn_csm_contour_map( outfile, ensSDtimeAvg, spreadMapRes )
		plots(1) = gsn_csm_contour_map( outfile, rmseTimeAvg, rmseMapRes )
		plots(2) = gsn_csm_contour_map( outfile, spreadSkillRatio, ratioMapRes )
		
		topPanelId = gsn_panel_return( outfile, plots(0:1), (/2,1/), twoPlotPanelRes )
		bottomPanelId = gsn_panel_return( outfile, plots(2), (/1,1/), bottomPanelRes )

		maximize_output( outfile, True )

		end 	; makeMapSpreadPlot

	if( isdefined("makeMapOutlierPlot") ) then undef("makeMapOutlierPlot") end if
	procedure makeMapOutlierPlot (				\\
	outfile[1] 				: graphic,	\\
	spreadSkillRatio[*][*] 	: numeric, 	\\
	outlierPercentage[*][*]	: numeric,	\\
	errorTimeAvg[*][*]		: numeric, 	\\
	ratioMapRes[1]			: logical,	\\
	outlierMapRes[1]		: logical, 	\\
	biasMapRes[1]			: logical, 	\\
	topPanelRes[1]			: logical,	\\
	middlePanelRes[1]		: logical,	\\
	bottomPanelRes[1]		: logical	\\
	)
		local plots, errorPanelId, mapPanelId, outlierPanelId
		begin

		plots = new(3, graphic)
		plots(0) = gsn_csm_contour_map( outfile, spreadSkillRatio, ratioMapRes )
		plots(1) = gsn_csm_contour_map( outfile, outlierPercentage, outlierMapRes )
		plots(2) = gsn_csm_contour_map( outfile, errorTimeAvg, biasMapRes )

		topPanelId = gsn_panel_return( outfile, plots(0), (/1,1/), topPanelRes )
		middlePanelId = gsn_panel_return( outfile, plots(1), (/1,1/), middlePanelRes )
		bottomPanelId = gsn_panel_return( outfile, plots(2), (/1,1/), bottomPanelRes )

		maximize_output( outfile, True )

		end 	; makeMapOutlierPlot

	if( isdefined("makeZonalSpreadPlot") ) then undef("makeZonalSpreadPlot") end if
	procedure makeZonalSpreadPlot (			\\
	outfile[1] 				: graphic,	\\
	ensSDtimeAvg[*][*]		: numeric,	\\
	rmseTimeAvg[*][*] 		: numeric,	\\
	spreadSkillRatio[*][*] 	: numeric, 	\\
	spreadZonalRes[1]		: logical,	\\
	rmseZonalRes[1]			: logical,	\\
	ratioZonalRes[1]		: logical,	\\
	twoPlotPanelRes[1]		: logical,	\\
	bottomPanelRes[1]		: logical	\\
	)
		local plots, errorPanelId, mapPanelId
		begin

		plots = new(3, graphic)
		plots(0) = gsn_csm_pres_hgt( outfile, ensSDtimeAvg, spreadZonalRes )
		plots(1) = gsn_csm_pres_hgt( outfile, rmseTimeAvg, rmseZonalRes )
		plots(2) = gsn_csm_pres_hgt( outfile, spreadSkillRatio, ratioZonalRes )

		topPanelId = gsn_panel_return( outfile, plots(0:1), (/2,1/), twoPlotPanelRes )
		bottomPanelId = gsn_panel_return( outfile, plots(2), (/1,1/), bottomPanelRes )

		maximize_output( outfile, True )

		end 	; makeZonalSpreadPlot

	if( isdefined("makeZonalOutlierPlot") ) then undef("makeZonalOutlierPlot") end if
	procedure makeZonalOutlierPlot (				\\
	outfile[1] 				: graphic,	\\
	spreadSkillRatio[*][*] 	: numeric, 	\\
	outlierPercentage[*][*]	: numeric,	\\
	errorTimeAvg[*][*]		: numeric, 	\\
	ratioZonalRes[1]		: logical,	\\
	outlierZonalRes[1]		: logical, 	\\
	biasZonalRes[1]			: logical, 	\\
	topPanelRes[1]			: logical,	\\
	middlePanelRes[1]		: logical,	\\
	bottomPanelRes[1]		: logical	\\
	)
		local plots, errorPanelId, mapPanelId, outlierPanelId
		begin

		plots = new(3, graphic)
		plots(0) = gsn_csm_pres_hgt( outfile, spreadSkillRatio, ratioZonalRes )
		plots(1) = gsn_csm_pres_hgt( outfile, outlierPercentage, outlierZonalRes )
		plots(2) = gsn_csm_pres_hgt( outfile, errorTimeAvg, biasZonalRes )

		topPanelId = gsn_panel_return( outfile, plots(0), (/1,1/), topPanelRes )
		middlePanelId = gsn_panel_return( outfile, plots(1), (/1,1/), middlePanelRes )
		bottomPanelId = gsn_panel_return( outfile, plots(2), (/1,1/), bottomPanelRes )

		maximize_output( outfile, True )

		end 	; makeZonalOutlierPlot

	if( isdefined("makeXYbinPlot") ) then undef("makeXYbinPlot") end if
	procedure makeXYbinPlot	(			\\
	outfile[1] 		:	graphic,		\\
	x[*]			:	numeric,		\\
	y[*]			:	numeric,		\\
	xBins[*] 		:	numeric,		\\
	yErrors[*]		:	numeric,		\\
	xyResIn[1] 		:	logical,		\\
	binRes[1]		:	logical,		\\
	regLineRes[1]	:	logical			\\
	)
		local outfile, errorBars, points, xx, xy, yx, yy, i, plot, xyRes, binRes, regY, regLineRes, pointRes, idealLineRes, ideal, linreg2
		begin
		
		xyRes = True
		
		xyRes@gsnDraw  = False         ; don't draw
		xyRes@gsnFrame = False         ; don't advance frame
		xyRes@gsnMaximize = True

		xyRes@xyMarkLineModes 	= (/"Markers"/)
		xyRes@xyMarkers 		= (/16/) 
		xyRes@xyMarkerColors 	= (/"blue"/)
		xyRes@xyMarkerSizeF 	= 0.002

		xyRes@tmXMajorGrid 					= True
		xyRes@tmXMajorGridLineDashPattern 	= 2
		xyRes@trXMinF = xBins(0)
		xyRes@trXMaxF = xBins( dimsizes(xBins)-1 )

		xyRes@tmYMajorGrid					= True
		xyRes@tmYMajorGridLineDashPattern 	= 2
		
		; pointRes = True
		; pointRes@gsMarkerIndex = xyRes@xyMarkers
		; pointRes@gsMarkerColor = xyRes@xyMarkerColors
		; pointRes@gsMarkerSizeF = xyRes@xyMarkerSizeF
		
		copy_VarAtts(xyResIn, xyRes)
					
		plot = gsn_csm_xy(outfile, x, y, xyRes)

		errorBars = new( (/ 2, dimsizes(x) /), graphic )
			
		do i=0, dimsizes(x)-1
			xx = (/ xBins(i)			, xBins(i+1) 		/)
			xy = (/ y(i)				, y(i) 				/)
			errorBars(0,i) = gsn_add_polyline(outfile, plot, xx, xy, binRes)
			
			yx = (/ x(i)				, x(i) 				/)
			yy = (/ y(i) - yErrors(i)	, y(i) + yErrors(i) /)
			errorBars(1,i) = gsn_add_polyline(outfile, plot, yx, yy, binRes)
		end do
		
		if( regLineRes .and. all( isatt( regLineRes, (/ "slope", "intercept" /) ) ) ) then
			regY = x * regLineRes@slope + regLineRes@intercept
			linreg = gsn_add_polyline(outfile, plot, x, regY, regLineRes)
			
			; corrected per Kolczynski et al. 2011
			regY = x * regLineRes@correctedSlope + regLineRes@correctedIntercept
			regLineRes@gsLineDashPattern = 2
			linreg2 = gsn_add_polyline(outfile, plot, x, regY, regLineRes)
		end if

		idealLineRes = True
		idealLineRes@gsLineColor = "grey"
		ideal = gsn_add_polyline(outfile, plot, (/ xyRes@trXMinF, xyRes@trXMaxF/), (/ xyRes@trYMinF, xyRes@trYMaxF/), idealLineRes )

		; points = gsn_add_polymarker(outfile, plot, x, y, pointRes)
		
		draw(outfile)
		frame(outfile)

		return

		end 	; makeXYbinPlot

	if( isdefined("makeSpreadSkillPlot") ) then undef("makeSpreadSkillPlot") end if 
	procedure makeSpreadSkillPlot ( \\
	outfile[1] 		: graphic,		\\
	ensSD[*]		: numeric, 		\\
	ensError[*]		: numeric, 		\\
	ensSize[1]		: integer,		\\
	binSize[1]		: integer, 		\\
	xyPlotRes[1] 	: logical, 		\\
	polyRes[1] 		: logical, 		\\
	regLineRes[1]	: logical 		\\
	)
		local bins, ensVarBinAvg, errorBinVar, slope, gammaParameter
		begin

		ensVar = ensSD^2
		bins = sortIntoBins( binSize, ensVar, ensError )

		ensVarBinAvg 	= bins@xBinAvg
		errorBinVar		= bins@yBinStdDev ^2

		if( regLineRes ) then
			slope = regline( ensVarBinAvg, errorBinVar )
			regLineRes@slope = slope
			regLineRes@intercept = slope@yintercept
			regLineRes@gsLineColor = "blue"

			xyPlotRes@gsnRightString = "y=" + sprintf( "%0.3f", slope) + "x + " + sprintf( "%0.3f", slope@yintercept )

			; corrected per Kolczynski et al. 2011
			gammaParameter = 2.0 * avg( ensVar^2 ) / variance( ensVar )
			regLineRes@correctedSlope = slope / ( 1 - gammaParameter/(ensSize+1) )
			regLineRes@correctedIntercept = slope@yintercept + slope * avg( ensVar ) / ( 1 - (ensSize+1)/gammaParameter )

			xyPlotRes@gsnRightString = "y=" + sprintf( "%0.3f", slope) + "x + " + sprintf( "%0.3f", slope@yintercept ) + "~C~" + \\
									   "y'=" + sprintf( "%0.3f", regLineRes@correctedSlope) + "x + " + sprintf( "%0.3f", regLineRes@correctedIntercept )
		end if

		makeXYbinPlot( outfile, ensVarBinAvg, errorBinVar, bins, errorBinVar*0, xyPlotRes, polyRes, regLineRes )

		end 	; makeSpreadSkillPlot


; ****************
; 	Main program
; ****************
begin

; read input file
input = asciiread( inputFilename, 32, "string" )
ensemblePaths			= str_split( input(0), "," )
ensGridTypes			= str_split( input(1), "," )
experimentNames			= str_split( input(2), "," )
startYYYYMMDDHHs		= str_split( input(3), "," )
endYYYYMMDDHHs			= str_split( input(4), "," )
forecastFrequenciesHr	= stringtointeger( str_split(  input(5), "," ) )
leadTimeHr				= stringtointeger( input(6) )

variableLabel			= input(7)
variableName			= input(8)
variableNiceName		= input(9)

avgVariableName			= str_sub_str(variableName, "P1", "P2")

verificationPath		= input(10)
verificationVariableName= input(11)
verificationType		= input(12)
verifGrid				= input(13)

useClimatology			= input(14).eq."True"
climatologyPath			= input(15)
climoVariableName		= input(16)
climatologyGrid 		= input(17)

outputPath 				= input(18)

spreadMapAttFilename 		= input(19)
spreadZonalAttFilename 		= input(20)
rmseMapAttFilename 			= input(21)
rmseZonalAttFilename 		= input(22)
ratioMapAttFilename 		= input(23)
ratioZonalAttFilename 		= input(24)
biasMapAttFilename 			= input(25)
biasZonalAttFilename 		= input(26)
outlierMapAttFilename 		= input(27)
outlierZonalAttFilename 	= input(28)
spreadSkillAttFilename 		= input(29)
normSpreadSkillAttFilename 	= input(30)
deleteAttFiles 				= input(31).eq."True"

experimentNameList 		= NewList("fifo")
ensSDtimeAvgList 		= NewList("fifo")
rmseTimeAvgList 		= NewList("fifo")
spreadSkillRatioList 	= NewList("fifo")
errorTimeAvgList 		= NewList("fifo")

ensSDlist 				= NewList("fifo")
errorList 				= NewList("fifo")
ensSDnormList			= NewList("fifo")
errorNormList			= NewList("fifo")

outlierPercentageList	= NewList("fifo")

spreadTimeAvgMax 	= 0.0
ratioMax 			= 0.0
errorTimeAvgMax 	= 0.0
spreadMax 			= 0.0
normSpreadMax		= 0.0
outlierMax			= 0.0

nExperiments = dimsizes(experimentNames)

; If we are syncing times, produce a list of times available for all experiments
if(SYNC_TIMES) then
	do e=0, nExperiments-1, 1
		ensemblePath		= ensemblePaths(e)
		ensGridType			= ensGridTypes(e)
		startYYYYMMDDHH		= startYYYYMMDDHHs(e)
		endYYYYMMDDHH		= endYYYYMMDDHHs(e)
		forecastFrequencyHr = forecastFrequenciesHr(e)

		; break input time strings into its different components
		startYear	= stringtointeger( str_get_cols( startYYYYMMDDHH, 0, 3) )
		startMonth	= stringtointeger( str_get_cols( startYYYYMMDDHH, 4, 5) )
		startDay	= stringtointeger( str_get_cols( startYYYYMMDDHH, 6, 7) )
		startHour	= stringtointeger( str_get_cols( startYYYYMMDDHH, 8, 9) )

		endYear		= stringtointeger( str_get_cols( endYYYYMMDDHH, 0, 3) )
		endMonth	= stringtointeger( str_get_cols( endYYYYMMDDHH, 4, 5) )
		endDay		= stringtointeger( str_get_cols( endYYYYMMDDHH, 6, 7) )
		endHour		= stringtointeger( str_get_cols( endYYYYMMDDHH, 8, 9) )

		; convert times into epoch-seconds
		startTime 	= cd_inv_calendar( startYear, startMonth, startDay, startHour, 0, 0, "seconds since 1970-1-1 00:00:00", 0 )
		endTime 	= cd_inv_calendar( endYear, endMonth, endDay, endHour, 0, 0, "seconds since 1970-1-1 00:00:00", 0 )
		forecastFrequency 	= forecastFrequencyHr * 3600;

		; build array with all of the initialization times
		nTimes = doubletointeger( (endTime - startTime) / forecastFrequency ) + 1
		if(nTimes .gt. 1)
			initTimes = fspan( startTime, endTime, nTimes )
		else
			initTimes = startTime
		end if
		copy_VarAtts( startTime, initTimes )

		; print( cd_string(initTimes, "%Y%N%D%H") )

		avgVariableName	= str_sub_str(variableName, "P1", "P2")

		validTimes = validGEFSmean( ensemblePath, ensGridType, initTimes, leadTimeHr, avgVariableName )
		nTimes = dimsizes(validTimes)

		if(e.eq.0) then
			syncTimes = validTimes
		else
			; only keep times already included in previously processed experiments
			syncTimes := validTimes( get1Dindex_Include( validTimes, syncTimes ) )
		end if

		; print( experimentNames(e) + ":" + cd_string(initTimes, "%Y%N%D%H") )
		; print("syncTimes:" + cd_string(syncTimes, "%Y%N%D%H") )

		delete(validTimes)

		validTimes = validGEFS( ensemblePath, ensGridType, members, initTimes, leadTimeHr, variableName )
		nTimes = dimsizes(validTimes)

		if(e.eq.0) then
			syncTimesFull = validTimes
		else
			; only keep times already included in previously processed experiments
			syncTimesFull := validTimes( get1Dindex_Include( validTimes, syncTimesFull ) )
		end if

		delete(validTimes)
		delete(initTimes)

	end do 	; e=0, nExperiments-1, 1

	print("Initialization times used for mean/spread:")
	print( " " + cd_string(syncTimes, "%Y%N%D%H") )

	print("Initialization times used for full ensemble:")
	print( " " + cd_string(syncTimesFull, "%Y%N%D%H") )

	delete(nTimes)
end if 	; (SYNC_TIMES)

nTimes = new(nExperiments, integer)
nTimesFull = new(nExperiments, integer)

do e=0, nExperiments-1, 1

	experimentName		= experimentNames(e)
	
	print( "Computing data for " + experimentName )

	ensemblePath		= ensemblePaths(e)
	ensGridType			= ensGridTypes(e)

	if(SYNC_TIMES) then
		initTimes = syncTimes
	else
		; build array with all of the initialization times
		startYYYYMMDDHH		= startYYYYMMDDHHs(e)
		endYYYYMMDDHH		= endYYYYMMDDHHs(e)
		forecastFrequencyHr = forecastFrequenciesHr(e)

		; break input time strings into its different components
		startYear	= stringtointeger( str_get_cols( startYYYYMMDDHH, 0, 3) )
		startMonth	= stringtointeger( str_get_cols( startYYYYMMDDHH, 4, 5) )
		startDay	= stringtointeger( str_get_cols( startYYYYMMDDHH, 6, 7) )
		startHour	= stringtointeger( str_get_cols( startYYYYMMDDHH, 8, 9) )

		endYear		= stringtointeger( str_get_cols( endYYYYMMDDHH, 0, 3) )
		endMonth	= stringtointeger( str_get_cols( endYYYYMMDDHH, 4, 5) )
		endDay		= stringtointeger( str_get_cols( endYYYYMMDDHH, 6, 7) )
		endHour		= stringtointeger( str_get_cols( endYYYYMMDDHH, 8, 9) )

		; convert times into epoch-seconds
		startTime 	= cd_inv_calendar( startYear, startMonth, startDay, startHour, 0, 0, "seconds since 1970-1-1 00:00:00", 0 )
		endTime 	= cd_inv_calendar( endYear, endMonth, endDay, endHour, 0, 0, "seconds since 1970-1-1 00:00:00", 0 )
		forecastFrequency 	= forecastFrequencyHr * 3600;


		nTimes(e) = doubletointeger( (endTime - startTime) / forecastFrequency ) + 1
		initTimes = fspan( startTime, endTime, nTimes(e) )
		copy_VarAtts( startTime, initTimes )

		; print( cd_string(initTimes, "%Y%N%D%H") )

		initTimes := validGEFSmean( ensemblePath, ensGridType, avgVariableName, initTimes, leadTimeHr )
		; print( cd_string(initTimes, "%Y%N%D%H") )

		delete(startYYYYMMDDHH)
		delete(endYYYYMMDDHH)
		delete(forecastFrequencyHr)
		delete(startYear)
		delete(startMonth)
		delete(startDay)
		delete(startHour)
		delete(endYear)
		delete(endMonth)
		delete(endDay)
		delete(endHour)
		delete(startTime)
		delete(endTime)
		delete(forecastFrequency)

	end if ; (.not.SYNC_TIMES)

	nTimes(e) = dimsizes(initTimes)
	leadTime 	= leadTimeHr * 3600

	; compute valid times
	validTimes = initTimes + leadTime
	copy_VarAtts( initTimes, validTimes )

	ensMean = readGEFSmean( ensemblePath, ensGridType, initTimes, leadTimeHr, avgVariableName )
	ensSD 	= readGEFSspread( ensemblePath, ensGridType, initTimes, leadTimeHr, avgVariableName )

	if(verificationType.eq."gdas") then
		obs	= readGDAS( verificationPath, verifGrid, validTimes, verificationVariableName )
	else if (verificationType.eq."consensus") then
		obs	= readConsensus( verificationPath, verifGrid, validTimes, verificationVariableName )
	else if (verificationType.eq."control") then
		obs = readGEFS( verificationPath, verifGrid, "gec00", validTimes, 0, verificationVariableName )
	else
		print("Fatal error: unknown verification type specified (plotMap.ncl:main)")
		status_exit(2)
	end if end if end if

	dimNames 	= getvardims(ensMean)
	timeDim 	= ind(dimNames.eq."initialization_time")

	; if(any(abs(ensMean).gt.1E5)) then
	; 	print("Possibly missing data for " + experimentName)
	; 	do t=0, nTimes(e)-1, 1
	; 		print(cd_string(initTimes(t), "%Y%N%D_%H" ) + "" )
	; 	end do
	; 	ensMean = ensMean@_FillValue
	; end if

	;
	; GDAS has more vertical levels than the ensemble forecast, so regrid in vertical if the field has a vertical component
	;
	obs := rectifyLevels(ensMean, obs)

	if( any( dimsizes(ensMean) .ne. dimsizes(obs) ) ) then
		print("FATAL: Dimensions of ensMean and obs do not match!")
		printVarSummary(ensMean)
		printVarSummary(obs)
	end if

	error = ensMean - obs
	copy_VarMeta(ensMean, error)
	error@long_name = "Error in ensemble mean"
	
	if( .not.ismissing(timeDim) ) then
		ensVar = ensSD^2
		copy_VarMeta(ensSD, ensVar)
		ensVar@long_name = "ensemble variance"
		ensSDtimeAvg 	= dim_avg_n_Wrap( ensVar, timeDim )
		ensSDtimeAvg	= sqrt( ensSDtimeAvg )
		rmseTimeAvg 	= dim_rmsd_n_Wrap( ensMean, obs, timeDim )
		errorTimeAvg 	= dim_avg_n_Wrap( error, timeDim )
		delete(ensVar)
	else
		ensSDtimeAvg 	= ensSD
		rmseTimeAvg 	= abs( ensMean - obs )
		errorTimeAvg 	= error
	end if 	; ( .not.ismissing(timeDim) )

	spreadSkillRatio = ( ensSDtimeAvg/where(rmseTimeAvg.gt.1E-9, rmseTimeAvg, 1E-9) - 1.0 ) * 100
	copy_VarMeta( ensSDtimeAvg, spreadSkillRatio )
	spreadSkillRatio@long_name 	= "Percent difference from ideal spread-skill ratio"
	spreadSkillRatio@units 		= "%"
	
	ListPush( experimentNameList, echo(experimentName) )
	ListPush( ensSDtimeAvgList, echo(ensSDtimeAvg) )
	ListPush( rmseTimeAvgList, echo(rmseTimeAvg) )
	ListPush( spreadSkillRatioList, echo(spreadSkillRatio) )
	ListPush( errorTimeAvgList, echo(errorTimeAvg) )
	ListPush( ensSDlist, echo(ensSD) )
	ListPush( errorList, echo(error) )

	ensSDtimeAvg_1D 	= ndtooned(ensSDtimeAvg)
	rmseTimeAvg_1D 		= ndtooned(rmseTimeAvg)
	spreadSkillRatio_1D = ndtooned(spreadSkillRatio)
	errorTimeAvg_1D 	= ndtooned(errorTimeAvg)
	ensSD_1D 			= ndtooned(ensSD)
	qsort(ensSDtimeAvg_1D)
	qsort(rmseTimeAvg_1D)
	qsort(spreadSkillRatio_1D)
	qsort(errorTimeAvg_1D)
	qsort(ensSD_1D)

	percentile = dimsizes(ensSDtimeAvg_1D)/100 * 92
	spreadTimeAvgMax 	= max( (/spreadTimeAvgMax, ensSDtimeAvg_1D(percentile), rmseTimeAvg_1D(percentile) /) )
	ratioMax 			= max( (/ratioMax, spreadSkillRatio_1D(percentile) /) )
	errorTimeAvgMax 	= max( (/errorTimeAvgMax, abs( errorTimeAvg_1D(percentile) ) /) )
	
	spreadMax  	= max( (/spreadMax, ensSD_1D( dimsizes(ensSD_1D)/100 * 98 ) /) )

	if(useClimatology) then
		climoMean 	= readClimoMean( climatologyPath, climatologyGrid, validTimes, climoVariableName )
		climoSD 	= readClimoStdDev( climatologyPath, climatologyGrid, validTimes, climoVariableName )

		;
		; Climo has fewer vertical levels than the ensemble forecast, so regrid in vertical if the field has a vertical component
		;
		ensMean_climo = rectifyLevels(climoMean, ensMean)
		ensSD_climo = rectifyLevels(climoMean, ensSD)
		error_climo = rectifyLevels(climoMean, error)

		ensSD_norm = ensSD_climo/climoSD
		error_norm = error_climo/climoSD
		copy_VarCoords(ensSD_climo, ensSD_norm)
		copy_VarCoords(error_climo, error_norm)

		ListPush( ensSDnormList, echo(ensSD_norm) )
		ListPush( errorNormList, echo(error_norm) )
		ensSDnorm_1D 		= ndtooned(ensSD_norm)
		qsort(ensSDnorm_1D)

		normSpreadMax = max( (/normSpreadMax, ensSDnorm_1D( dimsizes(ensSDnorm_1D)/100 * 98 ) /) )

		delete(climoMean)
		delete(climoSD)
		delete(ensMean_climo)
		delete(ensSD_climo)
		delete(error_climo)
		delete(ensSD_norm)
		delete(error_norm)
	
		delete(ensSDnorm_1D)
	end if ; useClimatology
	
	delete(initTimes)
	delete(validTimes)
	delete(ensMean)
	delete(ensSD)
	delete(obs)

	delete(error)

	delete(ensSDtimeAvg)
	delete(rmseTimeAvg)
	delete(spreadSkillRatio)
	delete(errorTimeAvg)

	delete(ensSDtimeAvg_1D)
	delete(rmseTimeAvg_1D)
	delete(spreadSkillRatio_1D)
	delete(errorTimeAvg_1D)
	delete(ensSD_1D)
	delete(percentile)

	delete(dimNames)
	delete(timeDim)
	
	if(SYNC_TIMES) then
		initTimes = syncTimesFull
	else
		; build array with all of the initialization times
		startYYYYMMDDHH		= startYYYYMMDDHHs(e)
		endYYYYMMDDHH		= endYYYYMMDDHHs(e)
		forecastFrequencyHr = forecastFrequenciesHr(e)

		; break input time strings into its different components
		startYear	= stringtointeger( str_get_cols( startYYYYMMDDHH, 0, 3) )
		startMonth	= stringtointeger( str_get_cols( startYYYYMMDDHH, 4, 5) )
		startDay	= stringtointeger( str_get_cols( startYYYYMMDDHH, 6, 7) )
		startHour	= stringtointeger( str_get_cols( startYYYYMMDDHH, 8, 9) )

		endYear		= stringtointeger( str_get_cols( endYYYYMMDDHH, 0, 3) )
		endMonth	= stringtointeger( str_get_cols( endYYYYMMDDHH, 4, 5) )
		endDay		= stringtointeger( str_get_cols( endYYYYMMDDHH, 6, 7) )
		endHour		= stringtointeger( str_get_cols( endYYYYMMDDHH, 8, 9) )

		; convert times into epoch-seconds
		startTime 	= cd_inv_calendar( startYear, startMonth, startDay, startHour, 0, 0, "seconds since 1970-1-1 00:00:00", 0 )
		endTime 	= cd_inv_calendar( endYear, endMonth, endDay, endHour, 0, 0, "seconds since 1970-1-1 00:00:00", 0 )
		forecastFrequency 	= forecastFrequencyHr * 3600;


		nTimesFull(e) = doubletointeger( (endTime - startTime) / forecastFrequency ) + 1
		initTimes = fspan( startTime, endTime, nTimesFull(e) )
		copy_VarAtts( startTime, initTimes )

		; print( cd_string(initTimes, "%Y%N%D%H") )

		initTimes := validGEFS( ensemblePath, ensGridType, members, initTimes, leadTimeHr, variableName )
		; print( cd_string(initTimes, "%Y%N%D%H") )

		delete(startYYYYMMDDHH)
		delete(endYYYYMMDDHH)
		delete(forecastFrequencyHr)
		delete(startYear)
		delete(startMonth)
		delete(startDay)
		delete(startHour)
		delete(endYear)
		delete(endMonth)
		delete(endDay)
		delete(endHour)
		delete(startTime)
		delete(endTime)
		delete(forecastFrequency)

	end if ; (.not.SYNC_TIMES)

	nTimesFull(e) = dimsizes(initTimes)

	; compute valid times
	validTimes = initTimes + leadTime
	copy_VarAtts( initTimes, validTimes )

	ens 	= readGEFS( ensemblePath, ensGridType, members, initTimes, leadTimeHr, variableName )
	
	if(verificationType.eq."gdas") then
		obs		= readGDAS( verificationPath, verifGrid, validTimes, verificationVariableName )
	else if (verificationType.eq."consensus") then
		obs		= readConsensus( verificationPath, verifGrid, validTimes, verificationVariableName )
	else if (verificationType.eq."control") then
		obs 	= readGEFS( verificationPath, verifGrid, "gec00", validTimes, 0, verificationVariableName )
	else
		print("Fatal error: unknown verification type specified (plotMap.ncl:main)")
		status_exit(2)
	end if end if end if

	; ====================
	; Calculate outliers
	; ====================
	dimNames  = getvardims(ens)
	memberDim = ind(dimNames.eq."ensemble_member")

	ensMin = dim_min_n_Wrap(ens, memberDim)
	ensMax = dim_max_n_Wrap(ens, memberDim)

	delete(dimNames)
	dimNames 	= getvardims(ensMin)
	leadDim 	= ind(dimNames.eq."lead_time_hr")
	ensMin 		:= dim_avg_n_Wrap(ensMin, leadDim)		; the lead_time_hr should be degenerate, so this just removes it without risking removing other degenerate dims with rm_single_dims
	ensMax 		:= dim_avg_n_Wrap(ensMax, leadDim)		; the lead_time_hr should be degenerate, so this just removes it without risking removing other degenerate dims with rm_single_dims

	delete(dimNames)
	dimNames 	= getvardims(ensMin)
	timeDim 	= ind(dimNames.eq."initialization_time")

	; The member pgrb files have even fewer levels, so reduce obs levels again
	obs := rectifyLevels(ensMin, obs)

	outlier = where( ensMin.gt.obs .or. ensMax.lt.obs, 1.0, 0.0 )
	copy_VarCoords( ensMin, outlier )
	outlierPercentage = dim_avg_n_Wrap( outlier, timeDim )
	outlierPercentage = outlierPercentage * 100.0

	outlierPercentage_1D= ndtooned(outlierPercentage)
	qsort(outlierPercentage_1D)	
	percentile 	= floattoint( dimsizes(outlierPercentage_1D)/100 * 92 )

	ListPush( outlierPercentageList, echo(outlierPercentage) )
	outlierMax			= max( (/outlierMax, abs( outlierPercentage_1D(percentile) ) /) )

	delete(ens)
	delete(obs)
	delete(ensMin)
	delete(ensMax)
	
	delete(initTimes)
	delete(leadTime)
	delete(validTimes)

	delete(outlierPercentage_1D)
	delete(percentile)

	delete(dimNames)
	delete(timeDim)
	delete(memberDim)
	delete(leadDim)

	delete(outlier)
	delete(outlierPercentage)

	delete(ensemblePath)
	delete(ensGridType)
	delete(experimentName)	
end do ; e=0, nExperiments-1, 1

; Load default plot resources
spreadMapRes 	= loadDefaultMapRes()
spreadZonalRes 	= loadDefaultZonalRes()

rmseMapRes 		= loadDefaultMapRes()
rmseZonalRes 	= loadDefaultZonalRes()

ratioMapRes		= loadDefaultMapRes()
ratioZonalRes 	= loadDefaultZonalRes()

biasMapRes 		= loadDefaultMapRes()
biasZonalRes 	= loadDefaultZonalRes()

outlierMapRes 	= loadDefaultMapRes()
outlierZonalRes = loadDefaultZonalRes()

topPanelRes		= loadDefaultPanelRes()
twoPlotPanelRes = loadDefaultPanelRes()
middlePanelRes 	= loadDefaultPanelRes()
bottomPanelRes 	= loadDefaultPanelRes()

xyRes 			= loadDefaultSpreadSkillRes()
xyNormRes 		= loadDefaultNormSpreadSkillRes()

contourLevels = nice_mnmxintvl( 0, spreadTimeAvgMax, 25, True )
spreadMapRes@gsnCenterString 	= "Ensemble Std Dev"
spreadMapRes@cnMinLevelValF		= contourLevels(0)
spreadMapRes@cnMaxLevelValF		= contourLevels(1)
spreadMapRes@cnLevelSpacingF	= contourLevels(2)
spreadMapRes@cnFillPalette 		= "WhiteBlueGreenYellowRed"

spreadZonalRes@gsnCenterString 	= "Ensemble Std Dev"
spreadZonalRes@cnMinLevelValF	= contourLevels(0)
spreadZonalRes@cnMaxLevelValF	= contourLevels(1)
spreadZonalRes@cnLevelSpacingF	= contourLevels(2)
spreadZonalRes@cnFillPalette 	= "WhiteBlueGreenYellowRed"

rmseMapRes@gsnCenterString 		= "RMSE of Ens Mean"
rmseMapRes@cnMinLevelValF		= contourLevels(0)
rmseMapRes@cnMaxLevelValF		= contourLevels(1)
rmseMapRes@cnLevelSpacingF		= contourLevels(2)
rmseMapRes@cnFillPalette 		= "WhiteBlueGreenYellowRed"

rmseZonalRes@gsnCenterString 	= "RMSE of Ens Mean"
rmseZonalRes@cnMinLevelValF		= contourLevels(0)
rmseZonalRes@cnMaxLevelValF		= contourLevels(1)
rmseZonalRes@cnLevelSpacingF	= contourLevels(2)
rmseZonalRes@cnFillPalette 		= "WhiteBlueGreenYellowRed"
delete(contourLevels)

contourLevels = nice_mnmxintvl( -100, 100, 25, True )
ratioMapRes@gsnCenterString 		= "% Diff from Ideal Spread-Skill Ratio"
ratioMapRes@cnMinLevelValF			= contourLevels(0)
ratioMapRes@cnMaxLevelValF			= contourLevels(1)
ratioMapRes@cnLevelSpacingF			= contourLevels(2)
ratioMapRes@cnFillPalette 			= "BlWhRe"

ratioZonalRes@gsnCenterString 		= "% Diff from Ideal Spread-Skill Ratio"
ratioZonalRes@cnMinLevelValF		= contourLevels(0)
ratioZonalRes@cnMaxLevelValF		= contourLevels(1)
ratioZonalRes@cnLevelSpacingF		= contourLevels(2)
ratioZonalRes@cnFillPalette 		= "BlWhRe"
delete(contourLevels)


contourLevels = nice_mnmxintvl( -errorTimeAvgMax, errorTimeAvgMax, 25, True )
biasMapRes@gsnCenterString  	= "Mean Error"
biasMapRes@cnMinLevelValF		= contourLevels(0)
biasMapRes@cnMaxLevelValF		= contourLevels(1)
biasMapRes@cnLevelSpacingF		= contourLevels(2)
biasMapRes@cnFillPalette 		= "BlWhRe"

biasZonalRes@gsnCenterString  	= "Mean Error"
biasZonalRes@cnMinLevelValF		= contourLevels(0)
biasZonalRes@cnMaxLevelValF		= contourLevels(1)
biasZonalRes@cnLevelSpacingF	= contourLevels(2)
biasZonalRes@cnFillPalette 		= "BlWhRe"
delete(contourLevels)

; print(outlierMax)
contourLevels = nice_mnmxintvl( -outlierMax, outlierMax, 25, True )
outlierMapRes@gsnCenterString 	= "Diff in % of Outliers from Uniform"
outlierMapRes@cnMinLevelValF	= contourLevels(0)
outlierMapRes@cnMaxLevelValF	= contourLevels(1)
outlierMapRes@cnLevelSpacingF	= contourLevels(2)
outlierMapRes@cnFillPalette 	= "BlWhRe"

outlierMapRes@gsnCenterString 	= "Diff in % of Outliers from Uniform"
outlierZonalRes@cnMinLevelValF	= contourLevels(0)
outlierZonalRes@cnMaxLevelValF	= contourLevels(1)
outlierZonalRes@cnLevelSpacingF	= contourLevels(2)
ReWhBlColormap = read_colormap_file("BlWhRe")
ReWhBlColormap = ReWhBlColormap(::-1,:)
outlierZonalRes@cnFillPalette 	= ReWhBlColormap

topPanelRes@gsnPanelTop			= 0.95
topPanelRes@gsnPanelBottom		= 0.65
topPanelRes@txFontHeightF 		= 0.015
topPanelRes@txConstantSpacingF	= 1.4

twoPlotPanelRes@gsnPanelTop			= 0.95
twoPlotPanelRes@gsnPanelBottom		= 0.35
twoPlotPanelRes@txFontHeightF 		= 0.015
twoPlotPanelRes@txConstantSpacingF	= 1.4

middlePanelRes@gsnPanelTop		= 0.65
middlePanelRes@gsnPanelBottom	= 0.35

bottomPanelRes@gsnPanelTop		= 0.35
bottomPanelRes@gsnPanelBottom	= 0.05

xyRes@trXMinF = 0.0
xyRes@trXMaxF = spreadMax^2
xyRes@trYMinF = xyRes@trXMinF
xyRes@trYMaxF = xyRes@trXMaxF

xyNormRes@trXMinF = 0.0
xyNormRes@trXMaxF = normSpreadMax^2
xyNormRes@trYMinF = xyNormRes@trXMinF
xyNormRes@trYMaxF = xyNormRes@trXMaxF

; Override plot resources from file
spreadMapRes 	= readPlotAtts( spreadMapRes, spreadMapAttFilename )
spreadZonalRes 	= readPlotAtts( spreadZonalRes, spreadZonalAttFilename )

rmseMapRes 		= readPlotAtts( rmseMapRes, rmseMapAttFilename )
rmseZonalRes 	= readPlotAtts( rmseZonalRes, rmseZonalAttFilename )

ratioMapRes 	= readPlotAtts( ratioMapRes, ratioMapAttFilename )
ratioZonalRes 	= readPlotAtts( ratioZonalRes, ratioZonalAttFilename )

biasMapRes 		= readPlotAtts( biasMapRes, biasMapAttFilename )
biasZonalRes 	= readPlotAtts( biasZonalRes, biasZonalAttFilename )

outlierMapRes 	= readPlotAtts( outlierMapRes, outlierMapAttFilename )
outlierZonalRes = readPlotAtts( outlierZonalRes, outlierZonalAttFilename )

xyRes 			= readPlotAtts( xyRes, spreadSkillAttFilename )
xyNormRes 		= readPlotAtts( xyNormRes, normSpreadSkillAttFilename )

do e=0, nExperiments-1, 1

	experimentName		= experimentNameList[e]
	ensSDtimeAvg 		= ensSDtimeAvgList[e]
	rmseTimeAvg 		= rmseTimeAvgList[e]
	spreadSkillRatio 	= spreadSkillRatioList[e]
	errorTimeAvg 		= errorTimeAvgList[e]
	ensSD 				= ensSDlist[e]
	error 				= errorList[e]
	outlierPercentage 	= outlierPercentageList[e]

	print( "Plotting data for " + experimentName )
	
	dimSizes = dimsizes(ensSDtimeAvg)
	
	spreadMapRes@gsnLeftString 	= nTimes(e) + " forecasts"
	rmseMapRes@gsnLeftString 	= nTimes(e) + " forecasts"
	ratioMapRes@gsnLeftString 	= nTimes(e) + " forecasts"
	biasMapRes@gsnLeftString 	= nTimes(e) + " forecasts"
	outlierMapRes@gsnLeftString = nTimesFull(e) + " forecasts"

	spreadZonalRes@gsnLeftString 	= nTimes(e) + " forecasts"
	rmseZonalRes@gsnLeftString 		= nTimes(e) + " forecasts"
	ratioZonalRes@gsnLeftString 	= nTimes(e) + " forecasts"
	biasZonalRes@gsnLeftString 		= nTimes(e) + " forecasts"
	outlierZonalRes@gsnLeftString 	= nTimes(e) + " forecasts"

	xyRes@gsnLeftString 		= nTimes(e) + " forecasts"
	xyNormRes@gsnLeftString 	= nTimes(e) + " forecasts"

	outDir = outputPath + "/" + experimentName + "/" + variableLabel

	if(useClimatology) then
		ensSD_norm 			= ensSDnormList[e]
		error_norm 			= errorNormList[e]
	end if ; useClimatology
	
	; Create plots

	if( dimsizes(dimSizes).eq.3 ) then
		nLevels = dimSizes(0)
		dimNames = getvardims(ensSDtimeAvg)
		ensembleLevels = ensSDtimeAvg&$dimNames(0)$

		if(ensembleLevels@units.eq."Pa") then
			ensembleLevels = ensembleLevels / 100
			ensembleLevels@units = "hPa"
		end if

		ensembleLevels := tointeger(ensembleLevels)
		
		do l=0, nLevels-1
			; Make plot of spread, rmse, and spread/rmse ratio
			outfileName =  outDir + "/" + experimentName + "_" + variableLabel + "_" + sprinti( "%04i", ensembleLevels(l) ) + "_f" + sprinti( "%03i", leadTimeHr ) + "_spread"
			outfile = gsn_open_wks( FILETYPE_MAP, outfileName )
			twoPlotPanelRes@txString = str_join( str_center_multi( (/	\\
									experimentName + " at " + sprinti( "%03i", leadTimeHr ) + " h", 	\\
									ensembleLevels(l) + "-hPa " + variableNiceName + " spread-skill"	\\
									/) ), "~C~" )
			makeMapSpreadPlot( outfile, ensSDtimeAvg(l,:,:), rmseTimeAvg(l,:,:), spreadSkillRatio(l,:,:), spreadMapRes, rmseMapRes, ratioMapRes, twoPlotPanelRes, bottomPanelRes )
			delete(outfile)
			if( FILETYPE_MAP.eq."png") then
				system("convert -limit thread 1 " + outfileName + ".png -resize " + PLOT_RESIZE + " -shave "+TRIM_WIDTH+"x0 " + outfileName + ".png")
			end if


			; Make plot of spread/rmse ratio, outlier percentage, and bias
			outfileName =  outDir + "/" + experimentName + "_" + variableLabel + "_" + sprinti( "%04i", ensembleLevels(l) ) + "_f" + sprinti( "%03i", leadTimeHr ) + "_outliers"
			outfile = gsn_open_wks( FILETYPE_MAP, outfileName )
			topPanelRes@txString = str_join( str_center_multi( (/	\\
									experimentName + " at " + sprinti( "%03i", leadTimeHr ) + " h", 	\\
									ensembleLevels(l) + "-hPa " + variableNiceName + " outlier percentage"	\\
									/) ), "~C~" )
			makeMapOutlierPlot( outfile, spreadSkillRatio(l,:,:), outlierPercentage(l,:,:), errorTimeAvg(l,:,:), ratioMapRes, outlierMapRes, biasMapRes, topPanelRes, middlePanelRes, bottomPanelRes )
			delete(outfile)
			if( FILETYPE_MAP.eq."png") then
				system("convert -limit thread 1 " + outfileName + ".png -resize " + PLOT_RESIZE + " -shave "+TRIM_WIDTH+"x0 " + outfileName + ".png")
			end if


			; Make reliability diagram
			outfileName =  outDir + "/" + experimentName + "_" + variableLabel + "_" + sprinti( "%04i", ensembleLevels(l) ) + "_f" + sprinti( "%03i", leadTimeHr ) + "_spreadSkill"
			outfile = gsn_open_wks( FILETYPE_SS, outfileName )
			xyRes@tiMainString = str_join( str_center_multi( (/	\\
									experimentName + " at " + sprinti( "%03i", leadTimeHr ) + " h",		\\
									ensembleLevels(l) + "-hPa " + variableNiceName + " spread-skill" 	\\
									/) ), "~C~" )
			makeSpreadSkillPlot( outfile, ndtooned( ensSD(:,l,:,:) ), ndtooned( error(:,l,:,:) ), N_ENS, BIN_SIZE, xyRes, True, True )
			delete(outfile)
			if( FILETYPE_MAP.eq."png") then
				; system("convert " + outfileName + ".png " + outfileName + ".png")
			end if
		end do ; l=0, nLevels-1

		delete(ensembleLevels)
		delete(dimNames)
		
		if(useClimatology) then			
			climoDimSizes = dimsizes(ensSD_norm)
			nClimoLevels = climoDimSizes(1)
			climoDimNames = getvardims(ensSD_norm)
			climoLevels = ensSD_norm&$climoDimNames(1)$

			if(climoLevels@units.eq."Pa") then
				climoLevels = climoLevels / 100
				climoLevels@units = "hPa"
			end if

			climoLevels := tointeger(climoLevels)
		
			do l=0, nClimoLevels-1
				outfileName =  outDir + "/" + experimentName + "_" + variableLabel + "_" + sprinti( "%04i", climoLevels(l) ) + "_f" + sprinti( "%03i", leadTimeHr ) + "_normSpreadSkill"
				outfile = gsn_open_wks( FILETYPE_SS, outfileName )
				xyNormRes@tiMainString = str_join( str_center_multi( (/	\\
										experimentName + " at " + sprinti( "%03i", leadTimeHr ) + " h", 			\\
										climoLevels(l) + "-hPa " + variableNiceName + " normalized spread-skill" 	\\
										/) ), "~C~" )
				makeSpreadSkillPlot( outfile, ndtooned( ensSD_norm(:,l,:,:) ), ndtooned( error_norm(:,l,:,:) ), N_ENS, BIN_SIZE, xyNormRes, True, True )
				delete(outfile)
				if( FILETYPE_MAP.eq."png") then
					; system("convert " + outfileName + ".png " + outfileName + ".png")
				end if
			end do 	; l=0, nClimoLevels-1
		end if ; useClimatology
		
		outfileName = outDir + "/" + experimentName + "_" + variableLabel + "_zonal_f" + sprinti( "%03i", leadTimeHr ) + "_spread"
		outfile = gsn_open_wks( FILETYPE_MAP, outfileName )
		topPanelRes@txString = str_join( str_center_multi( (/	\\
									experimentName + " at " + sprinti( "%03i", leadTimeHr ) + " h",		\\
									"Zonal Average " + variableNiceName + " spread-skill"			 	\\
									/) ), "~C~" )
		makeZonalSpreadPlot( outfile, dim_avg_Wrap(ensSDtimeAvg), dim_avg_Wrap(rmseTimeAvg), dim_avg_Wrap(spreadSkillRatio), spreadZonalRes, rmseZonalRes, ratioZonalRes, twoPlotPanelRes, bottomPanelRes )
		delete(outfile)
		if( FILETYPE_MAP.eq."png") then
			system("convert -limit thread 1 " + outfileName + ".png -resize " + PLOT_RESIZE + " -shave "+TRIM_WIDTH+"x0 " + outfileName + ".png")
		end if

		outfileName = outDir + "/" + experimentName + "_" + variableLabel + "_zonal_f" + sprinti( "%03i", leadTimeHr ) + "_outliers"
		outfile = gsn_open_wks( FILETYPE_MAP, outfileName )
		topPanelRes@txString = str_join( str_center_multi( (/	\\
									experimentName + " at " + sprinti( "%03i", leadTimeHr ) + " h",		\\
									"Zonal Average " + variableNiceName + " spread-skill"			 	\\
									/) ), "~C~" )
		makeZonalOutlierPlot( outfile, dim_avg_Wrap(spreadSkillRatio), dim_avg_Wrap(outlierPercentage), dim_avg_Wrap(errorTimeAvg), ratioZonalRes, outlierZonalRes, biasZonalRes, topPanelRes, middlePanelRes, bottomPanelRes )
		delete(outfile)
		if( FILETYPE_MAP.eq."png") then
			system("convert -limit thread 1 " + outfileName + ".png -resize " + PLOT_RESIZE + " -shave "+TRIM_WIDTH+"x0 " + outfileName + ".png")
		end if
		delete(nLevels)

	else
		; Make plot of spread, rmse, and spread/rmse ratio
		outfileName = outDir + "/" + experimentName + "_" + variableLabel + "_f" + sprinti( "%03i", leadTimeHr ) + "_spread"
		outfile = gsn_open_wks( FILETYPE_MAP, outfileName )
		topPanelRes@txString = str_join( str_center_multi( (/	\\
									experimentName + " at " + sprinti( "%03i", leadTimeHr ) + " h", 		\\
									variableNiceName + " spread-skill"									 	\\
									/) ), "~C~" )
		makeMapSpreadPlot( outfile, ensSDtimeAvg, rmseTimeAvg, spreadSkillRatio, spreadMapRes, rmseMapRes, ratioMapRes, twoPlotPanelRes, bottomPanelRes )
		delete(outfile)
		if( FILETYPE_MAP.eq."png") then
			system("convert -limit thread 1 " + outfileName + ".png -resize " + PLOT_RESIZE + " -shave "+TRIM_WIDTH+"x0 " + outfileName + ".png")
		end if

		; Make plot of spread/rmse ratio, outlier percentage, and bias
		outfileName = outDir + "/" + experimentName + "_" + variableLabel + "_f" + sprinti( "%03i", leadTimeHr ) + "_outliers"
		outfile = gsn_open_wks( FILETYPE_MAP, outfileName )
		topPanelRes@txString = str_join( str_center_multi( (/	\\
									experimentName + " at " + sprinti( "%03i", leadTimeHr ) + " h", 		\\
									variableNiceName + " outlier percentage"									 	\\
									/) ), "~C~" )
		makeMapOutlierPlot( outfile, spreadSkillRatio, outlierPercentage, errorTimeAvg, ratioMapRes, outlierMapRes, biasMapRes, topPanelRes, middlePanelRes, bottomPanelRes )
		delete(outfile)
		if( FILETYPE_MAP.eq."png") then
			system("convert -limit thread 1 " + outfileName + ".png -resize " + PLOT_RESIZE + " -shave "+TRIM_WIDTH+"x0 " + outfileName + ".png")
		end if

		; Make reliability diagram
		outfileName =  outDir + "/" + experimentName + "_" + variableLabel + "_f" + sprinti( "%03i", leadTimeHr ) + "_spreadSkill"
		outfile = gsn_open_wks( FILETYPE_SS, outfileName )
		xyRes@tiMainString = str_join( str_center_multi( (/	\\
									experimentName + " at " + sprinti( "%03i", leadTimeHr ) + " h",		\\
									variableNiceName + " spread-skill"						 			\\
									/) ), "~C~" )
		makeSpreadSkillPlot( outfile, ndtooned( ensSD(:,:,:) ), ndtooned( error(:,:,:) ), N_ENS, BIN_SIZE, xyRes, True, True )
		delete(outfile)
		if( FILETYPE_MAP.eq."png") then
			; system("convert " + outfileName + ".png " + outfileName + ".png")
		end if

		if(useClimatology) then
			outfileName =  outDir + "/" + experimentName + "_" + variableLabel + "_f" + sprinti( "%03i", leadTimeHr ) + "_normSpreadSkill"
			outfile = gsn_open_wks( FILETYPE_SS, outfileName )
			xyNormRes@tiMainString = str_join( str_center_multi( (/	\\
										experimentName + " at " + sprinti( "%03i", leadTimeHr ) + " h", 	\\
										variableNiceName + " normalized spread-skill"					 	\\
										/) ), "~C~" )
			makeSpreadSkillPlot( outfile, ndtooned( ensSD_norm(:,:,:) ), ndtooned( error_norm(:,:,:) ), N_ENS, BIN_SIZE, xyNormRes, True, True )
			delete(outfile)
			if( FILETYPE_MAP.eq."png") then
				; system("convert " + outfileName + ".png " + outfileName + ".png")
			end if
		end if ; useClimatology
	end if 	; nDims.gt.3

	delete(experimentName)
	delete(ensSDtimeAvg)
	delete(rmseTimeAvg)
	delete(spreadSkillRatio)

	delete(errorTimeAvg)
	delete(ensSD)
	delete(error)
	delete(outlierPercentage)

	delete(dimSizes)
	delete(outfileName)
	delete(outDir)
end do ; e=0, nExperiments-1, 1

if(deleteAttFiles) then
	system("rm " + spreadMapAttFilename)
	system("rm " + spreadZonalAttFilename)
	system("rm " + rmseMapAttFilename)
	system("rm " + rmseZonalAttFilename)
	system("rm " + ratioMapAttFilename)
	system("rm " + ratioZonalAttFilename)
	system("rm " + biasMapAttFilename)
	system("rm " + biasZonalAttFilename)
	system("rm " + outlierMapAttFilename)
	system("rm " + outlierZonalAttFilename)
	system("rm " + spreadSkillAttFilename)
	system("rm " + normSpreadSkillAttFilename)
end if

end ; main
