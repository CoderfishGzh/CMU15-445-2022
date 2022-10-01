
import os
import shutil
import sys

def comment_line(line):
    return "/* {} */ /* Commented by amalgamation script */".format(line)

def amalgamate(config):
    with open(config["output_file"], "w") as output_f:
        output_f.write("""/*
libfort

MIT License

Copyright (c) 2017 - 2020 Seleznev Anton

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

/* The file was GENERATED by an amalgamation script.*/
/* DO NOT EDIT BY HAND!!! */\n\n
#define FT_AMALGAMED_SOURCE /* Macros to make internal libfort functions static */\n
""")

        for hdr_file in config["header_files"]:
            with open(config["src_dir"] + "/" + hdr_file, "r") as input_f:
                txt = input_f.read()
                output_f.write('\n/********************************************************\n')
                output_f.write('   Begin of file "{}"\n'.format(hdr_file))
                output_f.write(' ********************************************************/\n\n')
                output_f.write(txt)
                output_f.write('\n/********************************************************\n')
                output_f.write('   End of file "{}"\n'.format(hdr_file))
                output_f.write(' ********************************************************/\n\n')

        for src_file in config["src_files"]:
            with open(config["src_dir"] + "/" + src_file, "r") as input_f:
                txt = input_f.read()
                output_f.write('\n/********************************************************\n')
                output_f.write('   Begin of file "{}"\n'.format(src_file))
                output_f.write(' ********************************************************/\n\n')
                output_f.write(txt)
                output_f.write('\n/********************************************************\n')
                output_f.write('   End of file "{}"\n'.format(src_file))
                output_f.write(' ********************************************************/\n\n')

    with open(config["output_file"]) as f:
        lines = f.readlines()

    
    forbidden_strings = map(lambda hdr_name: '#include "{}"'.format(hdr_name), config["header_files"])
           
    lines = map(lambda line: comment_line(line.strip()) + "\n" if line.strip() in forbidden_strings else line, lines)

    with open(config["output_file"], "w") as f:
        for line in lines:
            f.write(line)
        

        

def is_c_header_file(file):
    return ".h" in file


def is_c_source_file(file):
    return ".c" in file


def main():
    config = {}
    if len(sys.argv) >= 3 and sys.argv[1] == "-o":
        config["output_file"] = sys.argv[2]
    else:
        config["output_file"] = "./lib/fort.c"
    config["src_dir"] = "./src"
    all_files = os.listdir(config["src_dir"])
    config["src_files"] = sorted(filter(is_c_source_file, all_files))

    # config["header_files"] = filter(is_c_header_file, all_files)
    config["header_files"] = [
        "fort_utils.h",
        "vector.h",
        "wcwidth.h",
        "utf8.h",
        "string_buffer.h",
        "properties.h",
        "cell.h",
        "row.h",
        "table.h"
    ];
    amalgamate(config)

    # copy header files
    shutil.copyfile('./src/fort.h', './lib/fort.h')
    shutil.copyfile('./src/fort.hpp', './lib/fort.hpp')




if __name__ == '__main__':
    main()