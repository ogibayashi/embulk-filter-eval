module Embulk
  module Filter

    class EvalFilterPlugin < FilterPlugin
      Plugin.register_filter("eval", self)

      class NotFoundOutColumn < StandardError; end
      VERSION = '0.1.0'

      def self.transaction(config, in_schema, &control)
        # configuration code:
        task = {
          "eval_columns" => config.param("eval_columns", :array, default: []),
          "out_columns" => config.param("out_columns", :array, default: []),
          "add_columns" => config.param("add_columns", :array, default: []),
        }

        out_schema = out_schema(task['out_columns'], in_schema, task['add_columns'])

        yield(task, out_schema)
      end

      def self.out_schema(out_columns, in_schema, add_columns)
        schema = out_columns.map.with_index do |name, i|
          sch = in_schema.find { |sch| sch.name == name }

          unless sch
            raise NotFoundOutSchema, "Not found output schema: `#{name}'"
          end

          Embulk::Column.new(index: i, name: sch.name, type: sch.type, format: sch.format)
        end
        out_schema = schema.empty? ? in_schema : schema
        base_index = out_schema.size
        add_schema = add_columns.map.with_index do |v, i|
          Embulk::Column.new(index: base_index + i, name: v['name'], type: v['type'].to_sym)
        end
        out_schema + add_schema
      end

      def init
        @table = task["eval_columns"]
        @add_table = task["add_columns"]
      end

      def close
      end

      def add(page)
        page.each do |record|
          begin
            record = hash_record(record)

            result = {}

            record.each do |key, value|
              source = @table.find do |t|
                t.key?(key)
              end

              if source && source = source[key]
                result[key] = eval(source)
              else
                result[key] = value
              end
            end
            
            @add_table.each do |item|
              result[item["name"]] = type_convert(eval(item["value"]),item["type"],item["opts"])
            end
            
            page_builder.add(result.values)
          rescue
          end
        end
      end

      def finish
        page_builder.finish
      end

      def hash_record(record)
        Hash[in_schema.names.zip(record)]
      end

      def type_convert(v, field_type,opts={  })
        case field_type
          when "string"
            v
          when "long"
            v.to_i
          when "double"
            v.to_f
          when "timestamp"
            DateTime.strptime(v, opts["time_format"]).to_time
          when "boolean"
            BOOLEAN_TYPES.include? v.downcase
          else
            raise "unsupported type #{field_type}"
        end
      end

    end
  end
end
