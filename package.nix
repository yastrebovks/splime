{
  lib,
  buildPythonPackage,
  pythonOlder,
  nix-gitignore,

  # python package dependencies
  setuptools,
  wheel,
}:
let
  inherit (nix-gitignore) gitignoreSource;

  cleanNixSource = src: lib.cleanSourceWith {
    inherit src;
    filter = name: type: !(lib.hasSuffix ".nix" (baseNameOf (toString name)));
  };

in buildPythonPackage rec {
  pname = "spl";
  version = "0.0.0";
  pyproject = true;

  disabled = pythonOlder "3.13";
  src = cleanNixSource (lib.cleanSource (gitignoreSource [] ./.));

  build-system = [
    setuptools
    wheel
  ];

  dependencies = [];

  pythonImportsCheck = [
    # TODO: add package modules here
  ];

  meta = with lib; {
    description = "TODO";
  };
}
