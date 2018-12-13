package org.yyw.HadoopEDF;

import java.util.List;

/**
 * This class represents the complete content of an EDF-File.
 */
public class EDFParserResult
{
        EDFHeader header;
        EDFSignal signal;
        List<EDFAnnotation> annotations;

        public EDFHeader getHeader()
        {
                return header;
        }

        public EDFSignal getSignal()
        {
                return signal;
        }

        public List<EDFAnnotation> getAnnotations()
        {
                return annotations;
        }
}