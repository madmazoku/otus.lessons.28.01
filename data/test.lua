
-- parse line into pair of key,value
function parse(line)
    local key, value = line:match("([^\t]+)\t(.+)$")
    print("script   > key: " .. key .. "; value: " .. value)
    return key, value
end

-- collect pair of key,value into line
function collect(key, value)
    print("script   > key: " .. key .. "; values #: " .. #values)
    for _,v in ipairs(values) do
        print("\t\t" .. v)
    end
    return key
end

-- map pair of key,value into array of pairs of key,value for the stage #
function map(key, value, stage)
    local key_value = {
        {key, value}
    }
    return key_value
end

-- map pair of key, array of values into array of pairs of key,value for the stage #
function reduce(key, values, stage)
    local key_values = { }
    for _, v in ipairs(values)
        table.insert(key_values, {key, v})
    end
    return key_values
end
