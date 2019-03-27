; utilityFuncs.ncl
;
; Various general-purpose functions.
;
; Author: Walter Kolczynski, Jr.
;	Walter.Kolczynski@noaa.gov
;	Walter.Kolczynski@gmail.com
;
; Last modified 2014 Nov 21

;
; Creates an anonymous copy of a variable for inclusion in lists without causing issues if the variable is deleted.
;
if isdefined("echo") then undef("echo") end if
function echo(variable)
	local variable2
	begin
	variable2 = variable
	return variable2
	end ; echo

;
; Convert a list into a 1-D array
;
if( isdefined("listToArray") ) then undef("listToArray") end if
function listToArray( inputList: list, type: string )
	local array, i, n
	begin
	n = ListCount(inputList)
	array = new( n, type)
	do i=0, n-1
		array(i) = inputList[n-1-i]
	end do

	return array
	end 	;listToArray

;
; Removes duplicates from an 1-D array. The returned array will have an attribute, indexMapping, of length equal to the original array 
; 	that indicates the index of the new array that contains the value in the original array.
;
;	Example:
;	oldArray = (/ "A", "B", "C", "B", "E", "A", "D" /)
;	newArray = removeDuplicates(old)
;	
;	newArray: (/ "A", "B", "C", "E", "D" /)
;	newArray@indexMapping: (/ 0, 1, 2, 1, 3, 0, 4 /)
;
if( isdefined("removeDuplicates") ) then undef("removeDuplicates") end if
function removeDuplicates(array)
	local isIncomplete, indexMapping, newArrayList, nextInd, match, matchingInds, newArray
	begin

	isIncomplete = new( dimsizes(array), logical )
	isIncomplete = True
	indexMapping = new( dimsizes(array), integer )
	indexMapping = -1

	newArrayList = NewList("fifo")

	do while( any(isIncomplete) )
		nextInd = min( ind(isIncomplete) )						; get the next index that hasn't been mapped yet
		match = array(nextInd)									; get the value at that index
		matchingInds = ind( array.eq.match )					; get the indexes of all values that match
		isIncomplete(matchingInds) = False						; set those indexes as complete so we don't do them again
		indexMapping(matchingInds) = ListCount(newArrayList)	; record the new index for each of the old indexes that match
		ListPush(newArrayList, echo(match) )					; add the unique value to the list
		delete(matchingInds)
		delete(match)
	end do

	newArray = listToArray( newArrayList, typeof(array) )	; convert the list with unique values back into an array
	newArray@indexMapping = indexMapping					; add attribute with the old index => new index mapping

	return newArray

	end 	;removeDuplicates

;
; Removes duplicates from an 1-D array. The returned array will have an attribute, indexMapping, of length equal to the original array 
; 	that indicates the index of the new array that contains the value in the original array.
;
;	Example:
;	oldArray = (/ "A", "B", "C", "B", "E", "A", "D" /)
;	newArray = removeDuplicates(old)
;	
;	newArray: (/ "A", "B", "C", "E", "D" /)
;	newArray@indexMapping: (/ 0, 1, 2, 1, 3, 0, 4 /)
;
if( isdefined("removeDuplicatesAndSort") ) then undef("removeDuplicatesAndSort") end if
function removeDuplicatesAndSort(array : numeric)
	local isIncomplete, indexMapping, newArrayList, nextInd, match, matchingInds, newArray
	begin

	isIncomplete = new( dimsizes(array), logical )
	isIncomplete = True
	indexMapping = new( dimsizes(array), integer )
	indexMapping = -1

	sortIndexMapping = dim_pqsort( array, 1 )
	sortedArray = array( sortIndexMapping )

	newArrayList = NewList("fifo")

	do while( any(isIncomplete) )
		nextInd = min( ind(isIncomplete) )						; get the next index that hasn't been mapped yet
		match = sortedArray(nextInd)							; get the value at that index
		matchingInds = ind( sortedArray.eq.match )				; get the indexes of all values that match
		isIncomplete(matchingInds) = False						; set those indexes as complete so we don't do them again
		matchingInds = ind( array.eq.match )					; get the indexes of all values that match
		indexMapping(matchingInds) = ListCount(newArrayList)	; record the new index for each of the old indexes that match
		ListPush( newArrayList, echo(match) )					; add the unique value to the list
		delete(matchingInds)
		delete(match)
	end do

	newArray = listToArray( newArrayList, typeof(array) )	; convert the list with unique values back into an array
	newArray@indexMapping = indexMapping					; add attribute with the old index => new index mapping

	return newArray

	end 	;removeDuplicatesAndSort

;
;
;
if( isdefined("test_removeDuplicates") ) then undef("test_removeDuplicates") end if
procedure test_removeDuplicates()
	local oldArray, newArray
	begin

	oldArray = (/ "A", "B", "C", "B", "E", "A", "D" /)
	newArray = removeDuplicates(oldArray)

	print(newArray)
	print("")
	print("Mapping:")
	print(oldArray + " ==> " + newArray@indexMapping )

	end 	; test_removeDuplicates

if( isdefined("test_removeDuplicatesAndSort") ) then undef("test_removeDuplicatesAndSort") end if
procedure test_removeDuplicatesAndSort()
	local oldArray, newArray
	begin

	oldArray = (/ 10, 50, 20, 30, 20, 60, 10, 10 /)
	newArray = removeDuplicatesAndSort(oldArray)

	print(newArray)
	print("")
	print("Mapping:")
	print(oldArray + " ==> " + newArray@indexMapping )

	end 	; test_removeDuplicatesAndSort

if isdefined("get1Dindex_Include") then undef("get1Dindex_Include") end if
function get1Dindex_Include( array[*], include[*] )
	local indicies, newArray, err, oldErrLevel
	begin
		; turn off warnings to prevent bad parse warnings
		err = NhlGetErrorObjectId()
		getvalues err
			"errLevel" : oldErrLevel
		end getvalues
		setvalues err
			"errLevel" : "Fatal"
		end setvalues

		indicies = ispan( 0, dimsizes(array)-1, 1 )
		newArray = get1Dindex_Exclude( indicies, get1Dindex_Exclude(array, include) )

		setvalues err
			"errLevel" : oldErrLevel
		end setvalues

		return newArray
	end 	; get1Dindex_Include

if( isdefined("test_get1Dindex_Include") ) then undef("test_get1Dindex_Include") end if
procedure test_get1Dindex_Include()
	local a, b
	begin
	a = (/ 0, 1, 2, 3, 7 /)
	b = (/ 1, 3 /)
	print( get1Dindex_Include( a, b ) )
	end 	; test_get1Dindex_Include

if( isdefined("stringToType") ) then undef("stringToType") end if
function stringToType( input[1]: string )
	local err, oldErrLevel
	begin

	; turn off warnings to prevent bad parse warnings
	err = NhlGetErrorObjectId()
	getvalues err
		"errLevel" : oldErrLevel
	end getvalues
	setvalues err
		"errLevel" : "Fatal"
	end setvalues

	if( .not.ismissing( stringtointeger( input ) ) ) 	then
		setvalues err
			"errLevel" : oldErrLevel
		end setvalues
		return( stringtointeger( input ) )
	end if
	if( .not.ismissing( stringtofloat( input ) ) ) 		then
		setvalues err
			"errLevel" : oldErrLevel
		end setvalues
		return( stringtofloat( input ) )
	end if
	if( .not.ismissing( stringtodouble( input ) ) ) 	then
		setvalues err
			"errLevel" : oldErrLevel
		end setvalues
		return( stringtodouble( input ) )
	end if
	if( str_lower(input).eq."true" )					then
		setvalues err
			"errLevel" : oldErrLevel
		end setvalues
		return( True )
	end if
	if( str_lower(input).eq."false" )					then
		setvalues err
			"errLevel" : oldErrLevel
		end setvalues
		return( False )
	end if
	setvalues err
		"errLevel" : oldErrLevel
	end setvalues
	return input
	end 	; stringToType

if( isdefined("str_center_multi") ) then undef("str_center_multi") end if 
function str_center_multi( lines[*]: string )
	local lineLengths, padding
	begin

	lineLengths = strlen(lines)
	padding = 0.5 * ( max(lineLengths) - lineLengths )
	do i=0, dimsizes(lines)-1
		lines(i) = str_insert( "", "", stringtointeger( floor( padding(i) ) ) ) + lines(i) + str_insert( "", "", stringtointeger( ceil ( padding(i) ) ) )
	end do
	return lines
	end 	; str_center_multi


if( isdefined("slice") ) then undef("slice") end if
function slice(							\\
variable 							,	\\
dimension_number[1] 		: integer		,	\\
index[1] 			: integer			\\
)
	local dim_sizes, nDims, variable_1D, newVariable, newVariable_1D, blockSize, skip, nBlocks, offset, newDimSizes, b, blockStart, blockEnd
	begin

	if( ismissing(dimension_number) ) then
		print("Missing dimension number passed to slice(), returning unaltered variable")
		return(variable)
	end if

	dim_sizes = dimsizes(variable)
	nDims = dimsizes(dim_sizes)

	if( dimension_number .ge. nDims ) then
		print("Fatal Error in utilityFuncs.slice: index number " + dimension_number + " is greater than number of dimensions" + dimsizes(dim_sizes))
		status_exit(2)
	end if

	if( index .ge. dim_sizes(dimension_number) ) then
		print("Fatal Error in utilityFuncs.slice: index " + index + " is greater than size of the dimension" + dim_sizes(dimension_number))
		status_exit(2)
	end if

	variable_1D = ndtooned(variable)

	if( dimension_number .lt. nDims-1 )
		blockSize = product( dim_sizes(dimension_number+1:) )
	else
		blockSize = 1
	end if

	skip = blockSize * dim_sizes(dimension_number)

	if( dimension_number .eq. 0 )
		nBlocks = 1
	else
		nBlocks = product( dim_sizes(0:dimension_number-1))
	end if

	offset = blockSize * index

	; print("index number: " + dimension_number + " index: " + index)
	; print("offset: " + offset + " blockSize: " + blockSize + " skip: " + skip)

	newDimSizes = new(nDims-1, integer)
	if( dimension_number .gt. 0 )
		newDimSizes(0:dimension_number-1) = dim_sizes(0:dimension_number-1)
	end if

	if( dimension_number .lt. nDims-1 )
		newDimSizes(dimension_number:) = dim_sizes(dimension_number+1:)
	end if

	newVariable_1D = new(product(newDimSizes), typeof(variable))
	
	do b=0, nBlocks-1, 1
		blockStart = offset + skip * b
		blockEnd = blockStart + blockSize - 1
		; print("blockStart: " + blockStart + " blockEnd: " + blockEnd)
		newVariable_1D( b*blockSize:(b+1)*blockSize-1 ) = variable_1D(blockStart:blockEnd)
	end do

	newVariable = onedtond(newVariable_1D, newDimSizes)

	dimNames = getvardims(variable)
	newDimNames = new(nDims-1, string)
	if(dimension_number.gt.0) then
		newDimNames(0:dimension_number-1) = dimNames(0:dimension_number-1)
	end if

	if(dimension_number.lt.nDims-1) then
		newDimNames(dimension_number:) = dimNames(dimension_number+1:)
	end if

	do d=0, nDims-2, 1
		dimName = newDimNames(d)
		if(.not.ismissing(dimName)) then
			newVariable!d = dimName
			if( iscoord(variable,dimName) ) then
				newVariable&$dimName$ = variable&$dimName$
			end if
		end if
	end do

	copy_VarAtts(variable, newVariable)

	return newVariable

	end ; slice

if( isdefined("multislice") ) then undef("multislice") end if
function multislice(							\\
variable 							,	\\
dimension_number[1] 	: integer		,	\\
indicies[*] 			: integer			\\
)
	local dim_sizes, nDims, variable_1D, newVariable, newVariable_1D, block_size, skip, nBlocks, offset, newDimSizes, b, block_starts, block_start, block_end
	begin

	if( ismissing(dimension_number) ) then
		print("Missing dimension number passed to slice(), returning unaltered variable")
		return(variable)
	end if

	dim_sizes 	= dimsizes(variable)
	nDims 		= dimsizes(dim_sizes)
	nIndicies 	= dimsizes(indicies)

	if( dimension_number .ge. nDims ) then
		print("Fatal Error in utilityFuncs.slice: index number " + dimension_number + " is greater than number of dimensions" + dimsizes(dim_sizes))
		status_exit(2)
	end if

	if( any( indicies .ge. dim_sizes(dimension_number) ) ) then
		print("Fatal Error in utilityFuncs.slice: one of the incdicies is greater than size of the dimension" + dim_sizes(dimension_number))
		print(indicies + "")
		status_exit(2)
	end if

	variable_1D = ndtooned(variable)

	if( dimension_number .lt. nDims-1 )
		block_size = product( dim_sizes(dimension_number+1:) )
	else
		block_size = 1
	end if

	skip = block_size * dim_sizes(dimension_number)

	if( dimension_number .eq. 0 )
		n_blocks_per_offset = 1
	else
		n_blocks_per_offset = product( dim_sizes(0:dimension_number-1))
	end if
	nBlocks = n_blocks_per_offset * nIndicies

	newDimSizes 					= dim_sizes
	newDimSizes(dimension_number) 	= nIndicies

	offsets = block_size * indicies

	; print("index number: " + dimension_number + " index: " + index)
	; print("offset: " + offset + " block_size: " + block_size + " skip: " + skip)

	newVariable_1D = new(product(newDimSizes), typeof(variable))
	
	block_starts = ndtooned( conform_dims( (/n_blocks_per_offset,nIndicies/), offsets, 1) + conform_dims( (/n_blocks_per_offset,nIndicies/), skip * ispan(0,n_blocks_per_offset-1,1), 0 ) )

	do b=0, nBlocks-1, 1
		block_start = block_starts(b)
		block_end = block_start + block_size - 1
		; print("block_start: " + block_start + " block_end: " + block_end)
		newVariable_1D( b*block_size:(b+1)*block_size-1 ) = variable_1D(block_start:block_end)
	end do

	newVariable = onedtond(newVariable_1D, newDimSizes)

	dimNames = getvardims(variable)
	newDimNames = dimNames
	; if(dimension_number.gt.0) then
	; 	newDimNames(0:dimension_number-1) = dimNames(0:dimension_number-1)
	; end if

	; if(dimension_number.lt.nDims-1) then
	; 	newDimNames(dimension_number:) = dimNames(dimension_number+1:)
	; end if

	do d=0, nDims-1, 1
		dimName = newDimNames(d)
		if(.not.ismissing(dimName)) then
			newVariable!d = dimName
			if( iscoord(variable,dimName) ) then
				if(d.eq.dimension_number) then
					dim = variable&$dimName$
					newVariable&$dimName$ = dim(indicies)
				else 
					newVariable&$dimName$ = variable&$dimName$
				end if
			end if
		end if
	end do

	copy_VarAtts(variable, newVariable)

	return newVariable

	end ; slice


if( isdefined("test_slice") ) then undef("test_slice") end if
procedure test_slice()
	local variable, a, b, c, d
	begin

	variable = (/ 	(/								\\
						(/ "000", "001", "002", "003" /), 	\\
						(/ "010", "011", "012", "013" /), 	\\
						(/ "020", "021", "022", "023" /)	\\
	            	/), (/							\\
						(/ "100", "101", "102", "103" /), 	\\
						(/ "110", "111", "112", "113" /), 	\\
						(/ "120", "121", "122", "123" /)	\\
					/)								\\
				/)

	variable!0 = "A"
	variable!1 = "B"
	variable!2 = "C"

	variable&A = ispan(0,100,100)
	variable&B = ispan(0,20,10)
	variable&C = ispan(0,3,1)

	a = slice(variable, 0, 0)
	b = slice(variable, 0, 1)
	c = slice(variable, 1, 2)
	d = slice(variable, 2, 1)

	; print(variable)
	print(a)
	print(b)
	print(c)
	print(d)

	end ; test_slice

if( isdefined("test_multislice") ) then undef("test_multislice") end if
procedure test_multislice()
	local variable, a, b, c, d
	begin

	variable = (/ 	(/								\\
						(/ "000", "001", "002", "003" /), 	\\
						(/ "010", "011", "012", "013" /), 	\\
						(/ "020", "021", "022", "023" /)	\\
	            	/), (/							\\
						(/ "100", "101", "102", "103" /), 	\\
						(/ "110", "111", "112", "113" /), 	\\
						(/ "120", "121", "122", "123" /)	\\
					/)								\\
				/)

	variable!0 = "A"
	variable!1 = "B"
	variable!2 = "C"

	variable&A = ispan(0,100,100)
	variable&B = ispan(0,20,10)
	variable&C = ispan(0,3,1)

	a = multislice(variable, 0, 0)
	b = multislice(variable, 0, (/0,1/) )
	c = multislice(variable, 1, (/0,2/) )
	d = multislice(variable, 2, 1)

	; print(variable)
	print(a)
	print(b)
	print(c)
	print(d)

	end ; test_slice
