{ pkgs, inputs, ... }:

{
  packages = with pkgs; [
    postgresql.lib
  ];

  languages.rust = {
    enable = true;
    components = [
      "rustc"
      "cargo"
      "clippy"
      "rustfmt"
      "rust-analyzer"
    ];
    toolchain = {
      rustfmt = inputs.fenix.packages.${pkgs.system}.latest.rustfmt;
    };
  };

  git-hooks.hooks = {
    clippy.enable = true;
  };

  cachix.enable = false;
  dotenv.disableHint = true;
}
