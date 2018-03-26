#pragma once

extern "C" {
#include <lua5.2/lua.h>
#include <lua5.2/lauxlib.h>
#include <lua5.2/lualib.h>
}

#include <string>
#include <vector>

class Script {
private:
    lua_State* _lua_env;

public:
    Script() : _lua_env(nullptr) {
    }

    void init() {
        _lua_env = luaL_newstate();
        luaL_openlibs(_lua_env);
    }

    void done() {
        lua_close(_lua_env);
    }

    bool load(const std::string& file) {
        if (luaL_loadfile(_lua_env, file.c_str()) != 0 || lua_pcall(_lua_env, 0, 0, 0) != 0) {
            std::cerr << "Couldn't load file: " << lua_tostring(_lua_env, -1) << std::endl;
            return false;
        }        
        return true;
    }

    bool parse(const std::string& line, std::string& key, std::string& value) {
        lua_getglobal(_lua_env, "parse");
        lua_pushstring(_lua_env, line.c_str());
        int status = lua_pcall(_lua_env, 1 /* num params */, 2 /* num returns */, 0 /* err handler */);
        if (status) {
            std::cerr << "Failed to run parse: " << lua_tostring(_lua_env, -1) << std::endl;
            return false;
        }

        key = lua_tostring(_lua_env, -2);
        value = lua_tostring(_lua_env, -1);

        return true;
    }

    bool collect(std::vector<std::string>& lines, const std::string& key, const std::vector<std::string>& values) {
        lua_getglobal(_lua_env, "collect");
        lua_pushstring(_lua_env, key.c_str());

        lua_newtable(_lua_env);
        for(size_t n = 0; n < values.size(); ++n) {
            lua_pushstring(_lua_env, values[n].c_str());
            lua_rawseti(_lua_env, -2, n + 1);
        }

        int status = lua_pcall(_lua_env, 2 /* num params */, 1 /* num returns */, 0 /* err handler */);
        if (status) {
            std::cerr << "Failed to run collect: " << lua_tostring(_lua_env, -1) << std::endl;
            return false;
        }

        std::string line = lua_tostring(_lua_env, -1);
        lines.push_back(line);

        return true;
    }


};
