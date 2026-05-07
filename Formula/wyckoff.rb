class Wyckoff < Formula
  include Language::Python::Virtualenv

  desc "Wyckoff method quantitative analysis agent for A-shares"
  homepage "https://github.com/YoungCan-Wang/Wyckoff-Analysis"
  url "https://files.pythonhosted.org/packages/79/0b/b5725f634c6c5756164981fe21b05c07883919ebe944044d9cdbf436db19/youngcan_wyckoff_analysis-0.3.4.tar.gz"
  sha256 "307a31d44f31fa69530c1c4f27d758a7e0bc58d4a57f57073a076f855b21645a"
  license "AGPL-3.0-only"

  depends_on "python@3.11"

  def install
    virtualenv_install_with_resources
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/wyckoff --version")
  end
end
