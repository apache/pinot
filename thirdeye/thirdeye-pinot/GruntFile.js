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
        // files: {
        //   'dist/app.js': './src/main/resources/assets/javascript/HashService.js'
        // }
        files: [
          {
            expand: true,
            cwd: 'src/main/resources/assets/javascript/',
            src: ['**/*.js'],
            dest: 'dist/src'
          }
        ]
      }
    }
  });

  grunt.registerTask('default', ['babel']);
};
