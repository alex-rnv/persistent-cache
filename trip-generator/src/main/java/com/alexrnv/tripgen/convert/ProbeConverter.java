package com.alexrnv.tripgen.convert;


import com.alexrnv.tripgen.dto.DataObjects;

/**
 * Created: 2/10/16 6:41 PM
 * Author: alex
 */
public interface ProbeConverter<T> {
    DataObjects.Probe convert(T input);
}
