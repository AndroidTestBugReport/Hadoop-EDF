package org.yyw.HadoopEDF.ParallelProcessing.SequentialProgram;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class EDFAnnotation
{
        private double onSet = 0;
        private double duration = 0;
        private final List<String> annotations = new ArrayList<>();

        EDFAnnotation(String onSet, String duration, String[] annotations)
        {
                this.onSet = Double.parseDouble(onSet);
                if (duration != null && !Objects.equals(duration, ""))
                        this.duration = Double.parseDouble(duration);
                for (int i = 0; i < annotations.length; i++)
                {
                        if (annotations[i] == null || annotations[i].trim().equals(""))
                                continue;
                        this.annotations.add(annotations[i]);
                }
        }

        public double getOnSet()
        {
                return onSet;
        }

        public double getDuration()
        {
                return duration;
        }

        public List<String> getAnnotations()
        {
                return annotations;
        }

        @Override
        public String toString()
        {
                return "Annotation [onSet=" + onSet + ", duration=" + duration + ", annotations=" + annotations + "]";
        }
}
