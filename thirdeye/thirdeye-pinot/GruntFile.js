"use strict";
module.exports = (grunt) => {
  require('load-grunt-tasks')(grunt);

  // Project configuration
  grunt.initConfig({
    babel: {
        options: {
            sourceMap: true,
            presets: ['es2015']
        },
        dist: {
            files: {
                'dist/app.js': 'src/app.js'
            }
        }
    }
  });

  grunt.registerTask('default', ['babel']);


  // // Project configuration.
  // grunt.initConfig({
  //   pkg: grunt.file.readJSON('package.json'),
  //   uglify: {
  //     options: {
  //       banner: '/*! <%= pkg.name %> <%= grunt.template.today("yyyy-mm-dd") %> */\n'
  //     },
  //     build: {
  //       src: 'src/<%= pkg.name %>.js',
  //       dest: 'build/<%= pkg.name %>.min.js'
  //     }
  //   }
  // });

  // // Load the plugin that provides the "uglify" task.
  // grunt.loadNpmTasks('grunt-contrib-uglify');

  // // Default task(s).
  // grunt.registerTask('default', ['uglify']);

};
