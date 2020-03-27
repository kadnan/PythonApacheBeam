import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class ToLower(beam.DoFn):
    def process(self, element):
        return[{'Data': element.lower()}]


class ToReverse(beam.DoFn):
    def process(self, el):
        d = el['Data']
        return [d[::-1]]


if __name__ == '__main__':
    in_file = 'news.txt'
    out_file = 'processed'
    options = PipelineOptions()

    with beam.Pipeline(options=PipelineOptions()) as p:
        r = (
            p | beam.io.ReadFromText(in_file)
            | beam.ParDo(ToLower())
            | beam.ParDo(ToReverse())
            | beam.io.WriteToText(out_file, file_name_suffix='.txt')
        )

        result = p.run()
