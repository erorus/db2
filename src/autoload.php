<?php

spl_autoload_register(function($class) {
    $path = __DIR__.'/'.str_replace('\\',DIRECTORY_SEPARATOR, $class).'.php';
    if (file_exists($path)) {
        include $path;
    }
});

